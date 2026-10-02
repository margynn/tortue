# Scaling `plan()` to large torrents

## Problem

`plan()` (`tortue_lib/src/domain/swarm.rs`) rebuilds its entire candidate
list from scratch on every call:

```rust
let mut needed: Vec<usize> = self.pieces.needed_pieces().collect();
needed.sort_by_cached_key(|&piece| (...));
let blocks: Vec<BlockRange> = needed.iter()
    .flat_map(|&piece| self.pieces.unreceived_blocks(piece))
    .collect();
```

then walks it up to `max_depth` times (`while progressed { for replication
in 0..=max_depth { for &block in &blocks { ... } } }`, `max_depth` = number
of unchoked peers).

`plan()` runs on almost every event (`on_message`, `on_connected`,
`on_disconnected`, the block tick) — many times per second during an
active download.

Cost per call is `O(remaining_blocks × unchoked_peers)`, and it's paid
_again_ on every event regardless of how few new slots actually opened up.

- `cosmos_laundromat` (13,481 blocks): fine — worst case ~13.4k × 25 ≈
  340k simple comparisons per call.
- `ubuntu` (395,655 blocks): the same call costs ~30× more — worst case
  ~395k × 25 ≈ 9.9M comparisons (each involving a `HashMap` lookup) _per
  call_, called dozens of times a second. This is CPU-bound, not
  network-bound — matches the observed symptom exactly: `96 in flight`,
  25 peers connected, but only ~22 KiB/s actually moving.

## Design goals

1. **Simple, extensible code** — someone adding a new scheduling rule
   later (e.g. `AllowedFast`, per-piece deadlines) should be able to
   reason about one clear code path, not a tangle of caching invalidation
   rules.
2. **Performance that scales with the torrent** — cost per call should
   track `budget` (the number of request slots actually free right now —
   typically small, bounded by how many blocks resolved since the last
   call), not `remaining_blocks` (which can be in the hundreds of
   thousands).

## Recommended approach: two-mode scheduling

Split `plan()`'s work into the two situations that actually occur, instead
of one loop trying to handle both at once:

### 1. Normal mode — handing out first-time work

For nearly all of a download, there are far more blocks with **zero**
holders than there is budget to request them. In this mode we only need to
pop blocks off the front of a **pre-sorted queue** (rarest/suggested first)
and hand them to `pick_peer` until `budget` runs out.

- Maintain `never_requested: VecDeque<BlockRange>` as part of `Swarm`
  (or `PieceManager`), populated once and re-sorted only when priority-
  relevant state actually changes (a piece becomes suggested, a peer's
  rarity information shifts materially, etc.) — not on every message.
  Popping from the front and dropping items on assignment is O(1) per
  item.
- This path costs `O(budget)` per call, independent of how many blocks
  remain in the torrent.

### 2. Endgame mode — helping stragglers finish

`never_requested` only empties out when _every remaining block already has
at least one holder_ — which, by construction, only happens near the very
end of the download (this is exactly the ~20-block tail we saw with
`cosmos_laundromat`, and it's what the existing replication/endgame logic
in `plan()` is for).

- Only when `never_requested` is empty **and** budget remains do we fall
  back to the current depth-based replication scan — but by definition
  there are only a handful of blocks left to scan at that point (the
  stragglers), so the existing `O(remaining × unchoked_peers)` logic stays
  as-is here, unchanged, and it's cheap precisely because `remaining` is
  small whenever this path actually runs.

### Why this meets both goals

- **Simple**: two clearly-named modes, not one loop with implicit dual
  purpose. The existing replication logic is _reused_ for endgame, not
  rewritten — smaller diff, one thing to learn per mode.
- **Performant**: the expensive path (full-scan replication) only ever
  runs when the candidate set is already small. The hot path (normal
  downloading, 99% of the torrent) is `O(budget)`, so it no longer cares
  whether the torrent has 13k or 4M blocks.

## What doesn't need to change

- `pick_peer`, `holder_counts`, `BlockAssignments` — unaffected.
- The depth-based replication algorithm itself (`for replication in
0..=max_depth`) — kept for endgame mode exactly as it is today, since
  it's only ever asked to walk a short list at that point.

## Open question before implementing

`never_requested` needs to be rebuilt (or incrementally updated) when
piece priority changes — e.g. a new peer connects and shifts rarity, or a
piece gets marked suggested. Two options, in increasing order of
complexity:

- **(a) Rebuild on a cheap trigger, not every message**: recompute
  `never_requested` only when the _set_ of connected peers' bitfields
  changes materially (e.g. on `PeerConnected`/`PeerDisconnected`, or a
  simple debounce), not on every `Piece`/`Have` message. Simple, and
  those events are far less frequent than block arrivals.
- **(b) Fully incremental**: maintain rarity counters as bitfields/haves
  arrive and re-sort only the affected slice. More precise, more code.

Recommend **(a)** first — it already removes the dominant cost (rebuilding
on every block arrival) with a small, easy-to-follow change, and can be
tightened to (b) later only if measurements show it's still needed.

## Status

Design proposal — not yet implemented, pending approval.
