# Core

- Implement choking strategy

# BEP 6

### `SuggestPiece` (0x0D)

Advisory hint: "you should download this piece from me". Used for super-seeding and cache efficiency.

**What to do**: optional — could prioritize this piece in scheduling. Safe to ignore.

**Current**: `vec![]` ✅ (ignoring is valid per spec)

### When we **send choke** to a peer

Per spec, we MUST send `RejectRequest` for all their pending requests except allowed-fast pieces.

This requires tracking which requests the peer sent to us (i.e., `on_message_request` storing them). Currently we serve requests immediately and don't track them — so this point is moot for now.

### Sending `AllowedFast` to peers

Optional but recommended. We advertise pieces we'll serve even when choking. Not yet implemented.
