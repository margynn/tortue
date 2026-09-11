# Pool — `SuggestPiece` handling and supporting data structures

Companion to `bep6_fast_extension.md`. That doc marks `SuggestPiece` as
"ignoring is valid per spec" — this doc designs an actual implementation:
requesting the suggested piece directly from the peer that sent it, plus
the internal refactor needed to do that safely.

**Status: designed, not yet implemented.** `pool.rs` still has
`Message::SuggestPiece(_) => vec![]` and raw `HashMap` fields on `Pool`.

---

## Why

Per BEP 6, `Suggest Piece` means "you should request this piece from me" —
typically used for super-seeding or cache locality. The correct handling:
if we don't already have the suggested piece, send `Request`s for its
missing blocks straight to that peer, without going through the generic
rarest-first / random-peer selection in `schedule_requests`.

That request path still has to respect `MAX_IN_FLIGHT_PER_PEER`, the same
cap `schedule_requests`/`pick_peer` already enforce. Doing that cheaply
requires an O(1) way to ask "how many blocks are in flight for this peer
right now?" — today that count only exists as a `HashMap` rebuilt from
scratch at the top of every `schedule_requests` call (`pool.rs:404-407`).

Rather than duplicating that computation (or a cap check) in the new
handler, `block_assignments` becomes a small struct that maintains the
per-peer count as assignments are made and released. `availability` gets
the same treatment for consistency: it's the other `HashMap` mutated from
several handlers (`on_message_bitfield`, `on_message_have`,
`on_disconnected`, `schedule_requests`), and wrapping it removes the
inline `retain`/`get` calls scattered across them.

---

## `BlockAssignments`

Owns `BlockRef -> SocketAddr` plus the derived `SocketAddr -> usize`
in-flight count, kept in sync internally so callers can't update one
without the other.

```rust
#[derive(Default)]
struct BlockAssignments {
    by_block: HashMap<BlockRef, SocketAddr>,
    in_flight: HashMap<SocketAddr, usize>,
}

impl BlockAssignments {
    fn assign(&mut self, block_ref: BlockRef, addr: SocketAddr) {
        self.by_block.insert(block_ref, addr);
        *self.in_flight.entry(addr).or_default() += 1;
    }

    fn unassign(&mut self, block_ref: BlockRef) -> Option<SocketAddr> {
        let addr = self.by_block.remove(&block_ref)?;
        if let Some(count) = self.in_flight.get_mut(&addr) {
            *count -= 1;
            if *count == 0 {
                self.in_flight.remove(&addr);
            }
        }
        Some(addr)
    }

    fn in_flight_for(&self, addr: SocketAddr) -> usize {
        self.in_flight.get(&addr).copied().unwrap_or(0)
    }

    fn len(&self) -> usize {
        self.by_block.len()
    }

    /// Removes and returns every block currently assigned to `addr`.
    fn release_peer(&mut self, addr: SocketAddr) -> Vec<BlockRef> {
        let orphaned: Vec<BlockRef> = self
            .by_block
            .iter()
            .filter(|(_, p)| **p == addr)
            .map(|(k, _)| *k)
            .collect();
        for block_ref in &orphaned {
            self.unassign(*block_ref);
        }
        orphaned
    }
}
```

---

## `PieceAvailability`

Owns `PieceIndex -> HashSet<SocketAddr>` — which peers are known to have
which pieces.

```rust
#[derive(Default)]
struct PieceAvailability {
    by_piece: HashMap<PieceIndex, HashSet<SocketAddr>>,
}

impl PieceAvailability {
    fn record(&mut self, piece_index: PieceIndex, addr: SocketAddr) {
        self.by_piece.entry(piece_index).or_default().insert(addr);
    }

    fn remove_peer(&mut self, addr: SocketAddr) {
        self.by_piece.retain(|_, peers| {
            peers.remove(&addr);
            !peers.is_empty()
        });
    }

    /// Peers known to have `piece_index` (empty iterator if none known).
    fn peers_for(&self, piece_index: PieceIndex) -> impl Iterator<Item = &SocketAddr> {
        self.by_piece.get(&piece_index).into_iter().flatten()
    }

    /// Rarity for sorting: fewer known peers = rarer. Unknown pieces sort last.
    fn rarity(&self, piece_index: PieceIndex) -> usize {
        self.by_piece
            .get(&piece_index)
            .map_or(usize::MAX, |peers| peers.len())
    }
}
```

---

## `Pool` struct

```rust
pub struct Pool {
    metainfo: Arc<Metainfo>,
    peers: HashMap<SocketAddr, PeerState>,
    availability: PieceAvailability,       // was HashMap<PieceIndex, HashSet<SocketAddr>>
    block_assignments: BlockAssignments,   // was HashMap<BlockRef, SocketAddr>
    pieces: PieceManager,
}
```

`Pool::new` initializes both fields with `::default()` instead of
`HashMap::new()`.

---

## Call-site migration

Behavior is unchanged for all of these — this is a pure encapsulation
refactor, aside from the new `SuggestPiece` handler.

| Site                                           | Current (`pool.rs`)                                                                  | Becomes                                                                                                        |
| ---------------------------------------------- | ------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------- |
| `PoolSnapshot::blocks_in_flight` (:80)         | `self.block_assignments.len()`                                                       | unchanged call shape                                                                                           |
| `release_peer_blocks` (:109-120)               | manual filter + `.remove(&block_ref)` loop                                           | `for block_ref in self.block_assignments.release_peer(addr) { self.pieces.reset_block(block_ref); }`           |
| `on_message_bitfield` (:265-273)               | `self.availability.entry(piece).or_default().insert(addr)`                           | `self.availability.record(piece, addr)`                                                                        |
| `on_message_have` (:275-282)                   | `self.availability.entry(piece_index).or_default().insert(addr)`                     | `self.availability.record(piece_index, addr)`                                                                  |
| `on_disconnected` (:161-164)                   | inline `self.availability.retain(...)`                                               | `self.availability.remove_peer(addr)`                                                                          |
| `RejectRequest` handler (:238)                 | `self.block_assignments.remove(&block_ref)`                                          | `self.block_assignments.unassign(block_ref)`                                                                   |
| `on_message_piece` (:315)                      | `self.block_assignments.remove(&block_ref).is_none()`                                | `self.block_assignments.unassign(block_ref).is_none()`                                                         |
| `schedule_requests` in-flight setup (:404-407) | rebuilds a local `HashMap` every call                                                | deleted — read `self.block_assignments.in_flight_for(addr)` directly                                           |
| `schedule_requests` `can_schedule` (:409-412)  | checks local `in_flight` map                                                         | checks `self.block_assignments.in_flight_for(*addr)`                                                           |
| `schedule_requests` sort (:423-427)            | `self.availability.get(&piece).map_or(usize::MAX, \|p\| p.len())`                    | `self.availability.rarity(piece)`                                                                              |
| `schedule_requests` peer collection (:433-436) | `match self.availability.get(&piece_index) { Some(peers) => ..., None => continue }` | `peer_addrs.extend(self.availability.peers_for(piece_index).copied()); if peer_addrs.is_empty() { continue; }` |
| `pick_peer` (:385-400)                         | takes `in_flight: &HashMap<SocketAddr, usize>` param                                 | drops that param, reads `self.block_assignments.in_flight_for(*addr)` internally                               |
| `schedule_requests` assignment (:444/462)      | `self.block_assignments.insert(block_ref, addr)`                                     | `self.block_assignments.assign(block_ref, addr)`                                                               |

---

## New handler: `on_message_suggest_piece`

Added alongside the other `on_message_*` handlers (near `on_message_have`).

```rust
fn on_message_suggest_piece(&mut self, addr: SocketAddr, piece_index: usize) -> Vec<Output> {
    if !self.pieces.needed_pieces().any(|p| p == piece_index) {
        return vec![]; // already have it
    }

    let missing: Vec<BlockRange> = self.pieces.missing_blocks(piece_index).collect();
    let mut outputs = vec![];
    for block_range in missing {
        if self.block_assignments.in_flight_for(addr) >= Self::MAX_IN_FLIGHT_PER_PEER {
            break;
        }
        let block_ref = BlockRef::from(&block_range);
        self.block_assignments.assign(block_ref, addr);
        let _ = self.pieces.request_block(block_ref);
        outputs.push(Output::SendToPeer {
            addr,
            message: Message::Request {
                piece_index: block_range.piece_index,
                piece_offset: block_range.piece_offset,
                piece_len: block_range.piece_len,
            },
        });
    }
    outputs
}
```

Dispatch site (`pool.rs:228`, current `Message::SuggestPiece(_) => vec![]`):

```rust
Message::SuggestPiece(piece_index) => self.on_message_suggest_piece(addr, piece_index),
```

No choke check and no separate cap-check helper are needed here — the
break condition inline is the single place this cap is enforced for this
path, mirroring how `pick_peer` enforces it for `schedule_requests`.

---

## Reused patterns

- `needed_pieces` / `missing_blocks` / `request_block` / `BlockRef::from` —
  same `PieceManager` API `schedule_requests` already uses.
- Collecting `missing_blocks` into a `Vec` first avoids the
  immutable/mutable borrow conflict on `self.pieces`, same as
  `schedule_requests` already does.
- `MAX_IN_FLIGHT_PER_PEER` constant reused as-is — no new cap concept.
