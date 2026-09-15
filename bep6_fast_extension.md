# BEP 6 — Fast Extension

Source: https://www.bittorrent.org/beps/bep_0006.html

## Activation

Both peers must set bit 3 of the last reserved byte in the BEP 3 handshake:

```
reserved[7] |= 0x04
```

If only one side sets it, neither should use Fast Extension messages. The current code tracks this via `PeerState.fast`.

**Current behaviour**: if a Fast Extension message arrives from a peer that didn't negotiate it (`!state.fast`), the pool disconnects the peer. ✅ Correct.

---

## Key semantic change vs BEP 3

In BEP 3, a choke implicitly cancels all pending requests. In BEP 6:

> Every request is guaranteed to result in **exactly one response**: either `Piece` or `RejectRequest`.

This means:

- When **we choke** a peer → we MUST send `RejectRequest` for all their pending requests, **except** pieces in our Allowed Fast set.
- When **a peer chokes us** → our pending requests are NOT implicitly cancelled; we wait for either `Piece` or `RejectRequest` per request.

---

## Messages

### `HaveAll` (0x0E)

Sender has **all** pieces. Replaces `Bitfield` for seeders.

**What to do**: mark peer's bitfield as complete, then call `interested_or_request`.

```rust
Message::HaveAll => {
    state.bitfield.set_all();
    self.interested_or_request(addr)
}
```

**Current**: `vec![]` — peer is marked fast but their availability isn't recorded. ❌

---

### `HaveNone` (0x0F)

Sender has **no** pieces. Replaces `Bitfield` for fresh leechers.

**What to do**: nothing — bitfield is already empty by default.

```rust
Message::HaveNone => vec![]  // already the default state
```

**Current**: `vec![]` ✅ (functionally correct, no-op is right)

---

### `SuggestPiece` (0x0D)

Advisory hint: "you should download this piece from me". Used for super-seeding and cache efficiency.

**What to do**: optional — could prioritize this piece in scheduling. Safe to ignore.

**Current**: `vec![]` ✅ (ignoring is valid per spec)

---

### `RejectRequest` (0x10)

The peer refuses a request we sent. Contains `(piece_index, piece_offset, piece_len)`.

**What to do**:

1. Remove the block from `block_assignments`
2. Reset the block in `PieceManager` so it can be re-requested
3. Try to re-schedule from another peer

```rust
Message::RejectRequest { piece_index, piece_offset, piece_len } => {
    let block_ref = BlockRef { piece_index, piece_offset };
    self.block_assignments.remove(&block_ref);
    self.pieces.reset_block(block_ref);
    self.schedule_requests()
}
```

**Current**: `vec![]` — rejected blocks stay in-flight forever, causing stalls. ❌

---

### `AllowedFast` (0x11)

Peer advertises a piece they will serve **even while choking us**.

**What to do**: store in peer state. When we are choked by this peer and have an allowed-fast piece available, we can still request it.

```rust
// in PeerState
allowed_fast: HashSet<usize>,

// on AllowedFast(piece):
state.allowed_fast.insert(piece);
// then potentially schedule a request for this piece
```

The allowed fast set size is 10 pieces by default. Generation algorithm: SHA-1 of `(peer_ip_top3_octets || infohash)`, interpreted as 32-bit big-endian integers mod `num_pieces`, iterated until 10 unique pieces.

**Current**: `vec![]` — allowed fast pieces are ignored, so we can't download while choked. ❌

---

## What we need to emit

### When we **send choke** to a peer

Per spec, we MUST send `RejectRequest` for all their pending requests except allowed-fast pieces.

This requires tracking which requests the peer sent to us (i.e., `on_message_request` storing them). Currently we serve requests immediately and don't track them — so this point is moot for now.

### Sending `AllowedFast` to peers

Optional but recommended. We advertise pieces we'll serve even when choking. Not yet implemented.

---

## Summary: what's missing for full BEP 6

| Message         | Receive handling            | Priority |
| --------------- | --------------------------- | -------- |
| `HaveAll`       | Set full bitfield           | High     |
| `HaveNone`      | No-op ✅                    | —        |
| `SuggestPiece`  | Optionally prioritize       | Low      |
| `RejectRequest` | Reset block, reschedule     | High     |
| `AllowedFast`   | Store, request while choked | Medium   |

The two blocking issues for correctness are **`HaveAll`** (we miss seeder availability) and **`RejectRequest`** (rejected blocks stall forever).
