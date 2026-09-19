# Late block replies

### Problem

`on_message_piece` only accepts a `Piece` reply if the sender is still a
"holder" of that block:

```rust
if !self.block_assignments.is_holder(block_ref, addr) {
    return vec![];
}
```

`is_holder` currently checks the same `REQUEST_TIMEOUT` (5s) used to decide
when a stalled request should stop occupying a peer's scheduling slot
(`release_expired`, called from `plan()`). That timeout has to be short —
otherwise one slow peer can sit on scheduling budget and stall requests to
everyone else.

But a peer that answers slower than 5s isn't necessarily dead or
misbehaving — plain network/peer latency is enough to blow past it. Right
now, once the slot is freed:

- the block may get reassigned to another peer, **and**
- if the original (slow) peer's `Piece` reply arrives after that, it's
  silently dropped, because it's no longer a "holder".

That reply is real, requested, correctly-shaped data — we throw it away and
either wait on the new peer or re-request later, wasting the bandwidth the
slow peer already spent serving it.

### Why it's safe to accept it anyway

`piece_manager::receive_block` already treats a block that arrives after the
piece was completed by someone else as a no-op:

```rust
// An endgame duplicate must not re-emit a completion: that would
// write the piece and broadcast `Have` twice.
if !p.receive_block(block_ref.block_index(), data)? || !p.is_complete() {
    return Ok(None);
}
```

This is the same path that already handles endgame-mode duplicates (the
same block requested from multiple peers on purpose). So accepting a late
reply after reassignment costs nothing extra in correctness — the existing
dedup logic covers it.

### Proposed fix: split scheduling timeout from acceptance timeout

Two different questions, two different timeouts, both in
`block_assignment.rs`:

| Timeout | Question it answers | Used by |
|---|---|---|
| `REQUEST_TIMEOUT` (5s, existing) | "Has this request stalled long enough that the block should be offered to someone else?" | `holder_counts`, `free_slots_for`, `requests_in_flight`, `is_holder` (via `pick_peer`, to avoid double-assigning a peer already working on it) |
| `ACCEPT_TIMEOUT` (new, proposed 60s) | "Do we still trust a reply from this peer for this block?" | new `accepts_from()`, used only in `on_message_piece` |

Mechanically:

- `assign()` still records one `Instant` per `(peer, block)` — no new state,
  just two different windows read off the same timestamp.
- `is_holder(b, addr)` → `now < requested_at + REQUEST_TIMEOUT`. Unchanged
  meaning, still short.
- `accepts_from(b, addr)` (new) → `now < requested_at + ACCEPT_TIMEOUT`.
  Wider window; used in place of `is_holder` in `on_message_piece`'s
  validity check.
- `holder_counts()` / `free_slots_for()` / `requests_in_flight()`: filter to
  entries still inside `REQUEST_TIMEOUT` — a stalled request must not keep
  counting against a peer's budget or the block's replication count.
- `release_expired()` changes from "free stalled requests" to pure garbage
  collection: it only drops entries older than `ACCEPT_TIMEOUT`, bounding
  memory. Freeing the *scheduling* slot no longer depends on this being
  called — it falls out of the age filters above, so it's correct as soon
  as `plan()` runs, not just on a periodic sweep.

### Sequence this fixes

1. `t=0s` — block requested from peer A.
2. `t=5s` — no reply yet. `REQUEST_TIMEOUT` elapses: A no longer counts as a
   holder for scheduling; `plan()` is free to assign the block to peer B (or
   retry A).
3. `t=6s` — B completes the block. Piece marked received, `Have`
   broadcast.
4. `t=12s` — A's reply finally arrives. **Today:** dropped, `is_holder`
   false. **Proposed:** `accepts_from` still true (within 60s) →
   `receive_block` is called, sees the block already received, returns
   `Ok(None)` — harmless no-op, no double-write, no double `Have`.

### Open questions / things to decide before implementing

- **`ACCEPT_TIMEOUT` value.** 60s is a guess. Too long risks holding memory
  for peers that will never answer (bounded but non-zero cost); too short
  reintroduces the original problem for genuinely slow peers. Could also be
  expressed as a multiple of `REQUEST_TIMEOUT` (e.g. `12x`) instead of a
  fixed constant.
- **`downloaded_bytes` accounting.** `receive_block` unconditionally adds
  `data.len()` to `downloaded_bytes` before checking for duplicates, so a
  late duplicate still inflates the download-rate counter. This already
  happens today for real endgame duplicates, so this change doesn't make it
  worse — but worth flagging since a chatty slow peer could now trigger it
  more often than before (previously its data was just dropped before
  reaching this counter... actually it isn't reached today either, since
  `on_message_piece` returns early). Net effect: this change slightly
  increases how often we double-count downloaded bytes for a duplicate.
  Fixable separately (check-before-add) if it matters.
- **No behavior change for the common case.** When a peer replies within
  `REQUEST_TIMEOUT` (the vast majority of the time), nothing changes —
  `is_holder` is still true, still used, same as today.

### Status

Not yet implemented — draft only, pending approval.
