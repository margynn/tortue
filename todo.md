# BEP 6

### `SuggestPiece` (0x0D)

Advisory hint: "you should download this piece from me". Used for super-seeding and cache efficiency.

**What to do**: optional — could prioritize this piece in scheduling. Safe to ignore.

**Current**: `vec![]` ✅ (ignoring is valid per spec)

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

### When we **send choke** to a peer

Per spec, we MUST send `RejectRequest` for all their pending requests except allowed-fast pieces.

This requires tracking which requests the peer sent to us (i.e., `on_message_request` storing them). Currently we serve requests immediately and don't track them — so this point is moot for now.

### Sending `AllowedFast` to peers

Optional but recommended. We advertise pieces we'll serve even when choking. Not yet implemented.
