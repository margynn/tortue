# Endgame Mode

Source: https://wiki.theory.org/BitTorrentSpecification#End_Game (no formal BEP — it is a client-side strategy)

## The problem

Near the end of a download, every remaining block is in flight on a handful of peers. If one of them is slow, dead, or silently stalling, the download sits at 99.8% until the 30s request timeout expires (`REQUEST_TIMEOUT`, `pieces.rs:34`) — and may then re-pick the same slow peer.

Meanwhile the swarm has spare capacity we are not using: 20 unchoked peers × 16 slots = 320 request slots, 12 blocks left to fetch, 308 slots idle.

**Endgame mode**: once there is nothing left to request normally, request the remaining blocks from a second peer as well, and take whichever copy lands first.

---

## Trigger

No constant, no threshold, no state. The condition is:

> There is budget left after the normal pass, and the download is not complete.

Leftover budget means every requestable block is already in flight — which is exactly the endgame situation. `request_budget` (`pool.rs:492`) already computes it.

```rust
fn schedule_requests(&mut self) -> Vec<Output> {
    let mut budget = self.request_budget();
    if budget == 0 { return vec![]; }

    let mut outputs = self.request_pass(&mut budget, false);
    // Budget left over while the download is unfinished means everything
    // requestable is already in flight: spend the rest on duplicates.
    if budget > 0 && !self.pieces.is_complete() {
        outputs.extend(self.request_pass(&mut budget, true));
    }
    outputs
}
```

A tick that does not trigger the duplicate pass simply does nothing extra, so there is no cost to the condition flipping between ticks — no hysteresis and no persistent `endgame` flag are needed.

---

## Prerequisite: assignments indexed by peer

`BlockAssignments` currently holds two structures kept in sync by hand:

```rust
by_block: HashMap<BlockRef, SocketAddr>,   // one holder per block
in_flight: HashMap<SocketAddr, usize>,     // manual counter
```

`by_block` is 1:1, so endgame cannot work on top of it: `assign` (`pool.rs:520`) overwrites and decrements the previous holder, and `on_message_piece` (`pool.rs:297`) checks `assigned_to(block_ref) != Some(addr)` — which **discards the payload of every peer except the last one assigned**.

Replace both fields with a single index, keyed by peer:

```rust
struct BlockAssignments {
    by_peer: HashMap<SocketAddr, HashSet<BlockRef>>,
}

fn assign(&mut self, b: BlockRef, addr: SocketAddr) -> bool {
    self.by_peer.entry(addr).or_default().insert(b)
}

fn unassign(&mut self, b: BlockRef, addr: SocketAddr) {
    if let Some(blocks) = self.by_peer.get_mut(&addr) {
        blocks.remove(&b);
    }
}

fn is_holder(&self, b: BlockRef, addr: SocketAddr) -> bool {
    self.by_peer.get(&addr).is_some_and(|blocks| blocks.contains(&b))
}

fn in_flight_for(&self, addr: SocketAddr) -> usize {
    self.by_peer.get(&addr).map_or(0, |blocks| blocks.len())
}

fn release_peer(&mut self, addr: SocketAddr) -> HashSet<BlockRef> {
    self.by_peer.remove(&addr).unwrap_or_default()
}

/// Short-circuits on the first holder — used by the `reset_block` guard.
fn has_holder(&self, b: BlockRef) -> bool {
    self.by_peer.values().any(|blocks| blocks.contains(&b))
}

fn holder_count(&self, b: BlockRef) -> usize {
    self.by_peer.values().filter(|blocks| blocks.contains(&b)).count()
}
```

`by_block` was private to `impl BlockAssignments` (lines 502-566) — `Pool` only ever reached it through methods, and every one of those call sites already has the peer address in hand:

| Line | Current call                       | Becomes                              |
| ---- | ---------------------------------- | ------------------------------------ |
| 80   | `len()`                            | sum of set lengths                   |
| 110  | `release_peer(addr)`               | `remove(&addr)` — O(1)               |
| 223  | `unassign(block_ref)`              | `addr` is `on_message`'s parameter   |
| 297  | `assigned_to(b) != Some(addr)`     | `is_holder(b, addr)`                 |
| 300  | `unassign(block_ref)`              | `addr` is the parameter              |
| 386  | `has_capacity(addr)`               | derived from the set length          |
| 463  | `assign(b, addr)`                  | unchanged signature                  |
| 487  | `has_capacity(**addr)`             | derived from the set length          |
| 496  | `free_slots_for(s.addr)`           | derived from the set length          |

So the only `Pool` line that changes shape is 297.

### What this removes

- **`in_flight` and `decrement()`** (`pool.rs:503,534`): the count becomes derived (`HashSet::len`), therefore exact by construction. The defensive `saturating_sub` and the "phantom count" comment at `pool.rs:517-519` both lose their purpose — the desync class of bug they guard against can no longer be expressed.
- **the scan in `release_peer`** (`pool.rs:565`): a full walk of `by_block` plus a `Vec` allocation becomes one `remove`.

---

## Performance

The requirement is no regression. Per operation, with `P` = connected peers (tens) and `N` = total blocks in flight (`16 × P`, so hundreds to low thousands):

| Operation                | Called on                    | Now                        | With `by_peer`             |
| ------------------------ | ---------------------------- | -------------------------- | -------------------------- |
| `is_holder` / `assigned_to` | every `Piece` message     | 1 hash lookup              | 2 hash lookups             |
| `in_flight_for`          | `pick_peer`, `request_budget`| 1 lookup + copy            | 1 lookup + `len()`         |
| `assign`                 | every request sent           | 2-3 hash ops               | 2 hash ops                 |
| `unassign`               | every block received         | 2 hash ops                 | 2 hash ops                 |
| `release_peer`           | every choke / disconnect     | **O(N) scan + Vec alloc**  | **O(1)**                   |
| `blocks_in_flight`       | UI refresh                   | O(1)                       | O(P) adds                  |
| `has_holder`             | new, `reset_block` guard     | —                          | O(P) short-circuited       |
| `holder_count`           | new, duplicate pass only     | —                          | O(P)                       |

The one hot-path operation that gets more expensive is `is_holder`: one extra hash, on the path of a message that also carries a 15 KiB memcpy and eventually a SHA-1 over the whole piece. It is not measurable there.

`release_peer` is the operation that actually improves, and it matters: BitTorrent rechokes every 10 seconds, so chokes are frequent, and today each one walks the entire in-flight map.

The two new scans are both O(P), not O(N), and neither is on a hot path:

- `has_holder` runs in `release_peer_blocks`, once per released block. Worst case a peer with 16 blocks disconnects: 16 × P ≈ 800 `contains` calls. It short-circuits on the first holder found.
- `holder_count` runs only in the duplicate pass, which by its trigger condition only runs when the unreceived-block set is small.

Allocations: `by_peer` holds `P` long-lived `HashSet`s that grow to their steady-state capacity and stay there, against today's two maps whose `by_block` entries churn on every block. Fewer allocations over the life of a download, not more.

---

## Scheduling

`schedule_requests`'s current loop (`pool.rs:408-473`) becomes `request_pass`, parameterised by one bool. Two lines differ between the passes:

```rust
const MAX_HOLDERS_PER_BLOCK: usize = 2;

fn request_pass(&mut self, budget: &mut usize, duplicates: bool) -> Vec<Output> {
    // ... unchanged: rng, peer_addrs buffer, rarest-first `needed` sort ...

    for piece_index in needed {
        if *budget == 0 { break; }
        peer_addrs.clear();
        peer_addrs.extend(self.availability.peers_for(piece_index).copied());
        if peer_addrs.is_empty() { continue; }

        let blocks: Vec<BlockRange> = if duplicates {
            self.pieces.unreceived_blocks(piece_index).collect()
        } else {
            self.pieces.missing_blocks(piece_index).collect()
        };

        for block_range in blocks {
            if *budget == 0 { break; }
            let block_ref = BlockRef::from(&block_range);
            if duplicates
                && self.block_assignments.holder_count(block_ref) >= MAX_HOLDERS_PER_BLOCK
            {
                continue;
            }
            if let Some(addr) = self.pick_peer(&peer_addrs, block_ref, duplicates, &mut rng) {
                *budget -= 1;
                outputs.push(self.send_request(addr, block_range));
            }
        }
    }
    outputs
}
```

`pick_peer` gains one filter clause, active only in the duplicate pass:

```rust
&& (!duplicates || !self.block_assignments.is_holder(block_ref, **addr))
```

It must stay gated. In the normal pass, `missing_blocks` yields blocks whose request has timed out and whose holder is still recorded; if that holder were excluded and it is the only peer advertising the piece, the block would become permanently unrequestable. Keeping the filter off in pass 1 preserves today's behaviour exactly.

### `unreceived_blocks` vs `missing_blocks`

`missing_blocks` filters on the request timeout (`pieces.rs:233`), so a block requested 5s ago is not yielded. The duplicate pass needs the opposite: re-request blocks that are `Requested` and recent. Same `BlockRange` mapping, different predicate — factor the `flat_map` of `missing_blocks` (`pieces.rs:98`) into a helper taking an index iterator.

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

## The invariant to hold

> `pieces.reset_block()` may only be called when the block has no remaining holder.

Otherwise one peer choking resets to `Missing` a block another peer is actively sending. Three call sites:

- `release_peer_blocks` (`pool.rs:109`) — reached from `on_connected`, `on_disconnected`, `on_message_choke`
- `RejectRequest` (`pool.rs:223`)
- the malformed-block path of `on_message_piece` (`pool.rs:307`)

Each becomes `if !self.block_assignments.has_holder(block_ref) { self.pieces.reset_block(block_ref); }`.

The second copy of a block that arrives after the first has been accepted needs no special handling: `Piece::receive_block` short-circuits on an already-`Received` block (`pieces.rs:257`) and returns `Ok`.

---

## Deliberately left out

**`Cancel` on the redundant requests.** The textbook endgame cancels the duplicates as soon as one copy lands. At `MAX_HOLDERS_PER_BLOCK = 2`, with the duplicate pass only active once the unreceived set is small, the wasted download is on the order of a megabyte per torrent — not worth the extra plumbing (`Message::Cancel` needs a `piece_len` that `BlockRef` does not carry). Revisit if it shows up in the transfer stats.

**Ranking duplicate targets by peer throughput.** Sending the duplicate to the fastest available peer is the natural refinement, but we collect no per-peer speed stats yet. Random selection among eligible peers is good enough to start.

**Freeing the slot of a timed-out request.** Today a request that times out leaves its assignment in place, so the peer's pipeline slot stays occupied indefinitely while the block is handed to someone else. This is a pre-existing issue, orthogonal to endgame; folding a fix into this change would confuse the two.

---

## Bugs in the current WIP

Why the in-progress version at `pool.rs:440-468` does not work, for the record:

1. `budget` is never decremented in the endgame branch (`pool.rs:456`, commented out), so the outer `if budget == 0 { break }` never fires: 5 requests are emitted per block across _every_ remaining piece.
2. `is_endgame = budget > missing.len()` compares the global budget against the blocks missing **in the current piece only** (~17 for a 256 KiB piece). With 20 unchoked peers, `budget ≈ 320`, so endgame is on from the first block of the download.
3. `pick_peer` is called 5 times in a row and can return the same peer each time; and with the 1:1 `by_block` map only the last assignment survives, so 4 of the 5 responses are dropped by the `assigned_to` check.

---

## Tests

**`BlockAssignments`**

- two peers holding the same block; `in_flight_for` stays exact across `unassign`
- `release_peer` removes only that peer's blocks, leaving the other holder intact
- `has_holder` is false only once the last holder is gone

**`Pool`**

- no duplicate request while the normal pass still has blocks to hand out
- leftover budget on an incomplete download → the remaining blocks get a second holder, capped at `MAX_HOLDERS_PER_BLOCK`
- the duplicate never goes to a peer already holding the block
- a timed-out block whose only advertising peer is its current holder is still re-requested (the gated filter)
- a `Piece` from a non-holder is ignored
- a `Choke` from one of two holders does **not** reset the block
- a malformed block from one of two holders does **not** reset the block

---

## Summary

| Change                                        | Where                        | Priority |
| --------------------------------------------- | ---------------------------- | -------- |
| `by_block` + `in_flight` → `by_peer`          | `pool.rs:501-576`            | Blocking |
| `assigned_to` → `is_holder`                   | `pool.rs:297`                | Blocking |
| `reset_block` guarded by `has_holder`         | 3 call sites in `pool.rs`    | Blocking |
| `request_pass(duplicates: bool)`              | `pool.rs:408`                | High     |
| Leftover-budget trigger                       | `pool.rs:408`                | High     |
| `unreceived_blocks`                           | `pieces.rs:98`               | High     |
| `pick_peer`: gated non-holder filter          | `pool.rs:475`                | High     |
| `Cancel` the redundant requests               | `pool.rs:289`                | Later    |
| Rank duplicate targets by throughput          | `pool.rs:475`                | Later    |

One new constant: `MAX_HOLDERS_PER_BLOCK = 2`.
