# Endgame Mode

Source: https://wiki.theory.org/BitTorrentSpecification#End_Game (no formal BEP — it is a client-side strategy)

## The problem

Near the end of a download, the remaining blocks are all in flight on a handful of peers. If one of those peers is slow, dead, or silently stalling, the download sits at 99.8% until the 30s request timeout expires (`REQUEST_TIMEOUT`, `pieces.rs:34`) — and then possibly re-picks the same slow peer.

At that point the swarm has spare capacity we are not using: we have, say, 20 unchoked peers × 16 slots = 320 request slots, and only 12 blocks left to fetch. 308 slots idle.

**Endgame mode**: once the remaining block count drops below available capacity, request each remaining block from _several_ peers at once, and `Cancel` the redundant requests as soon as the first copy lands.

---

## Two distinct quantities

The trigger condition needs to separate what the current code conflates:

| Quantity     | Definition                             | Decides                       |
| ------------ | -------------------------------------- | ----------------------------- |
| **capacity** | `16 × nb_unchoked_peers`               | _whether_ we enter endgame    |
| **budget**   | `Σ free_slots_for(peer)` over unchoked | _how many_ requests this tick |

`budget` already exists as `Pool::request_budget` (`pool.rs:492`). Capacity is the structural ceiling — it does not shrink as requests go in flight, so it is the right thing to compare against the remaining work.

```rust
const ENDGAME_MAX_BLOCKS: usize = 64;
const ENDGAME_DUPLICATES: usize = 3;

fn request_capacity(&self) -> usize {
    self.peers.values().filter(|s| !s.peer_choking).count()
        * BlockAssignments::MAX_IN_FLIGHT_PER_PEER
}

fn update_endgame(&mut self) -> bool {
    let missing = self.pieces.blocks_missing();
    let threshold = self.request_capacity().min(ENDGAME_MAX_BLOCKS);
    // Hysteresis: capacity moves every time a peer chokes or joins, and we do
    // not want to toggle the strategy on each tick.
    self.endgame = if self.endgame {
        missing <= threshold * 2
    } else {
        missing > 0 && missing <= threshold
    };
    self.endgame
}
```

`ENDGAME_MAX_BLOCKS` is not optional. In a large swarm `capacity` can reach 16 000; `missing <= capacity` would then fire with 240 MiB still to download and we would duplicate the whole torrent.

`blocks_missing()` on `PieceManager` is `blocks_total() - blocks_received()`.

---

## Prerequisite: 1:N block assignments

`BlockAssignments.by_block` is currently `HashMap<BlockRef, SocketAddr>` — one holder per block. Endgame cannot work on top of it:

- `assign` (`pool.rs:520`) decrements the previous holder and overwrites, so duplicate requests are never tracked.
- `on_message_piece` (`pool.rs:297`) checks `assigned_to(block_ref) != Some(addr)` and therefore **discards the payload from every peer except the last one assigned**.

```rust
struct BlockAssignments {
    by_block: HashMap<BlockRef, HashSet<SocketAddr>>,
    in_flight: HashMap<SocketAddr, usize>,
}

fn assign(&mut self, b: BlockRef, addr: SocketAddr) -> bool {
    if self.by_block.entry(b).or_default().insert(addr) {
        *self.in_flight.entry(addr).or_default() += 1;
        return true;
    }
    false // already assigned to this peer — do not re-request
}

fn holders(&self, b: BlockRef) -> &HashSet<SocketAddr>;   // empty set if absent
fn holder_count(&self, b: BlockRef) -> usize;
fn is_holder(&self, b: BlockRef, addr: SocketAddr) -> bool;
fn unassign_one(&mut self, b: BlockRef, addr: SocketAddr) -> bool; // true if no holder left
fn unassign_all(&mut self, b: BlockRef) -> Vec<SocketAddr>;
```

### The invariant this creates

> `pieces.reset_block()` may only be called when the block has **no remaining holder**.

Otherwise one peer choking resets to `Missing` a block that two other peers are actively sending. Call sites to fix:

- `release_peer_blocks` (`pool.rs:109`) — used by `on_connected`, `on_disconnected`, `on_message_choke`
- `RejectRequest` (`pool.rs:223`)
- the malformed-block path of `on_message_piece` (`pool.rs:307`)

`snapshot().blocks_in_flight` keeps its meaning: `by_block.len()` is still the number of distinct blocks in flight, not the number of requests.

---

## Scheduling in two passes

Cleaner than an inline `if is_endgame` inside the block loop, and it falls out correctly by construction: the duplicate pass only ever spends _leftover_ budget, which is precisely the "capacity exceeds remaining work" condition.

```rust
fn schedule_requests(&mut self) -> Vec<Output> {
    let mut budget = self.request_budget();
    if budget == 0 { return vec![]; }

    let mut outputs = self.schedule_rarest_first(&mut budget); // current loop, unchanged
    if self.update_endgame() && budget > 0 {
        outputs.extend(self.schedule_duplicates(&mut budget));
    }
    outputs
}

fn schedule_duplicates(&mut self, budget: &mut usize) -> Vec<Output> {
    let mut targets: Vec<BlockRange> = self.pieces.needed_pieces()
        .flat_map(|p| self.pieces.unreceived_blocks(p))
        .collect();
    // Least-covered blocks first: those are the ones holding up the tail.
    targets.sort_by_key(|b| self.block_assignments.holder_count(BlockRef::from(b)));

    let mut rng = rand::rng();
    let mut outputs = vec![];
    for range in targets {
        if *budget == 0 { break; }
        let block_ref = BlockRef::from(&range);
        let held = self.block_assignments.holders(block_ref).clone();
        let extra = ENDGAME_DUPLICATES.saturating_sub(held.len());
        let candidates: Vec<SocketAddr> = self.availability
            .peers_for(range.piece_index)
            .filter(|a| !held.contains(a))
            .copied()
            .collect();
        for addr in self.pick_peers(&candidates, range.piece_index, extra, &mut rng) {
            *budget -= 1;
            outputs.push(self.send_request(addr, range));
            if *budget == 0 { break; }
        }
    }
    outputs
}
```

### `pick_peers` vs `pick_peer`

Two changes over `pool.rs:475`:

1. **Exclude peers that already hold the block.** Sending the same request twice to one peer is pure waste and gets connections dropped by strict clients.
2. **Pick _n_ distinct peers** — `choose_multiple(rng, n)`, not `n` successive `choose(rng)` calls (which can return the same peer every time).

### `unreceived_blocks` vs `missing_blocks`

`missing_blocks` filters on the request timeout (`pieces.rs:233`): a block requested 5s ago is not yielded. Endgame needs the opposite — re-request blocks that are `Requested` and recent. Same `BlockRange` mapping, different predicate, so factor the `flat_map` of `missing_blocks` (`pieces.rs:98`) into a helper taking an index iterator:

```rust
impl Piece {
    fn unreceived_blocks(&self) -> impl Iterator<Item = usize> + '_ {
        self.blocks.iter().enumerate().filter_map(|(i, s)| {
            (!matches!(s, BlockState::Received { .. })).then_some(i)
        })
    }
}
```

---

## Cancel

This is half the point of endgame. Without it, duplication multiplies wasted download bandwidth by `ENDGAME_DUPLICATES`.

Cancels are emitted **after** the block validates — if `receive_block` fails on a malformed payload, we want the other copies still in flight.

```rust
if !self.block_assignments.is_holder(block_ref, addr) {
    return vec![]; // unsolicited, or a duplicate that lost the race
}

match self.pieces.receive_block(block_ref, data) {
    Err(_) => {
        if self.block_assignments.unassign_one(block_ref, addr) {
            self.pieces.reset_block(block_ref);
        }
        vec![]
    },
    Ok(event) => {
        let cancels: Vec<Output> = self.block_assignments
            .unassign_all(block_ref)
            .into_iter()
            .filter(|p| *p != addr)
            .map(|p| Output::SendToPeer {
                addr: p,
                message: Message::Cancel { piece_index, piece_offset, piece_len },
            })
            .collect();
        // ... existing match on event, with `cancels` prepended to the outputs
    },
}
```

`Message::Cancel` needs `piece_len`, which `BlockRef` does not carry. Either expose `PieceManager::block_len(block_ref)` or store the full `BlockRange` in the assignment — the former keeps `BlockAssignments` keyed on `BlockRef`.

A duplicate arriving after the cancel is already handled: `unassign_all` cleared the holders, so `is_holder` returns `false` and the payload is dropped. `Piece::receive_block` also short-circuits on an already-`Received` block (`pieces.rs:257`), which is a second line of defence.

On the serving side, `Message::Cancel => vec![]` (`pool.rs:195`) stays correct: we answer `Request` synchronously in `on_message_request`, so there is no outbound queue to purge.

---

## Bugs in the current WIP

For reference, the three reasons the in-progress version at `pool.rs:440-468` does not work:

1. `budget` is never decremented in the endgame branch (`pool.rs:456`, commented out), so the outer `if budget == 0 { break }` never fires and 5 requests are emitted per block across _every_ remaining piece.
2. `is_endgame = budget > missing.len()` compares the global budget against the blocks missing **in the current piece only** (~17 for a 256 KiB piece). With 20 unchoked peers, `budget ≈ 320`, so endgame is on from the first block of the download.
3. `pick_peer` is called 5 times in a row and can return the same peer each time; and the 1:1 `by_block` map means only the last assignment survives, so 4 of the 5 responses are discarded by the `assigned_to` check.

---

## Tests

Split by unit, then merge what overlaps:

**`BlockAssignments`**

- multiple holders for one block; `in_flight` per peer stays accurate across `unassign_one` / `unassign_all`
- `release_peer` removes only that peer's holdings, leaving other holders intact

**`update_endgame`**

- no endgame at 50% completion even with 100 peers (`ENDGAME_MAX_BLOCKS` clamp)
- endgame triggers once `missing` drops under the threshold
- no flapping when a single peer chokes (hysteresis)

**`Pool`**

- one block requested from 3 peers → 2 `Cancel` outputs when the first copy arrives
- a `Piece` from a non-holder is ignored
- a `Choke` from one of 2 holders does **not** reset the block
- a malformed block from one of 2 holders does **not** reset the block

---

## Summary

| Change                                          | Where                        | Priority |
| ----------------------------------------------- | ---------------------------- | -------- |
| `by_block` → `HashMap<BlockRef, HashSet<Addr>>` | `pool.rs` `BlockAssignments` | Blocking |
| `reset_block` only when no holder left          | 4 call sites in `pool.rs`    | Blocking |
| Global `update_endgame` + hysteresis            | `pool.rs` `Pool`             | High     |
| Two-pass `schedule_requests`                    | `pool.rs:408`                | High     |
| `Cancel` on first copy received                 | `pool.rs:289`                | High     |
| `unreceived_blocks` (ignores request timeout)   | `pieces.rs`                  | High     |
| `pick_peers`: distinct, non-holder peers        | `pool.rs:475`                | High     |
| Rank duplicate targets by upload speed          | `pool.rs`                    | Later    |

The last one needs per-peer throughput stats we do not collect yet; random distinct selection is good enough to start.
