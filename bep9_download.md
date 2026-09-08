# BEP 9 — Magnet Link Download Architecture

## Overview

Two entry points for the `download` command:

- `.torrent` file → metadata already available, go straight to piece download
- magnet link → fetch metadata first via BEP 9, then same piece download flow

## CLI

```
tortue download <path|magnet>   # auto-detect
tortue download magnet:?xt=urn:btih:<hash>&tr=<tracker>&dn=<name>
tortue download ./file.torrent
```

`main.rs`: the `path: PathBuf` argument becomes `source: String`, detected at runtime:

- starts with `magnet:` → parse as magnet
- otherwise → read as file path

## Magnet Link Format

```
magnet:?xt=urn:btih:<info_hash_hex_or_base32>
       &tr=<tracker_url>      (0 or more, repeatable)
       &dn=<display_name>     (optional)
       &x.pe=<peer_addr>      (0 or more, repeatable)
```

Minimum required: `xt` (info hash). Trackers optional (DHT fallback, not implemented yet).

Parsed into (✅ implemented in `domain/magnet.rs`):

```rust
pub struct MagnetLink {
    pub info_hash: InfoHash,
    pub trackers: Vec<String>,
    pub name: Option<String>,
    pub peers: Vec<String>,   // x.pe direct peer addresses
}
```

## Download Flow

### With .torrent (current)

```
torrent bytes
    → Metainfo::try_from(&bytes)
    → Arc<Metainfo>
    → Pool + TrackerIO + PeerConnector
    → piece download
```

### With magnet link (new)

```
MagnetLink { info_hash, trackers, peers, .. }
    → MetadataIO::run(info_hash, trackers, peers)   ← new
    → Vec<u8> raw info bytes (SHA1-validated)
    → Metainfo::try_from(&bytes)
    → Arc<Metainfo>                                  ← same from here
    → Pool + TrackerIO + PeerConnector
    → piece download
```

The application layer (`application/magnet.rs`) handles the branching. Once `Arc<Metainfo>` is obtained, the rest is identical.

---

## Implementation

### 1. `domain/metadata.rs` — Pure state machine (🚧 skeleton done)

#### Change needed: `MessageReceived` must carry `Message`, not `UtMetadataMessage`

The BEP 10 extension handshake arrives as `Message::ExtensionHandshake` through the normal
message channel (see `peer_io.rs::spawn_reader`). `Metadata` must see it to extract
`metadata_size` and the peer's `ut_metadata` ext id. Change:

```rust
// BEFORE
MessageReceived { addr: SocketAddr, message: UtMetadataMessage },

// AFTER
MessageReceived { addr: SocketAddr, message: Message },
```

#### `Metadata` state

```rust
pub struct Metadata {
    info_hash: InfoHash,
    total_size: Option<usize>,
    pieces: Vec<Option<Vec<u8>>>,
    // maps peer addr → their ut_metadata ext_id (from their BEP 10 handshake)
    peers: HashMap<SocketAddr, u8>,
}
```

`pending_peers` is not needed — pieces are assigned lazily when a peer connects and its
extension handshake is received.

#### `Metadata::new(info_hash: InfoHash) -> Self`

Initialize with empty state. `total_size` and `pieces` are unknown until the first peer
sends its BEP 10 handshake with `metadata_size`.

#### `Metadata::on_input(input: MetadataInput) -> Vec<MetadataOutput>`

- **`PeerConnected { addr, peer_id }`** — no output yet; wait for `ExtensionHandshake`.

- **`MessageReceived { addr, message: Message::ExtensionHandshake(hs) }`**
  - Extract `hs.extensions.get("ut_metadata")` → peer's ext_id; if absent, ignore peer (no ut_metadata support).
  - Extract `hs.metadata_size`:
    - If `total_size` is None: initialize `pieces = vec![None; ceil(metadata_size / 16384)]`.
    - If already set: validate it matches; disconnect peer if inconsistent.
  - Insert peer into `self.peers` with their ext_id.
  - Return `Request` messages for all missing pieces, each as:
    ```rust
    MetadataOutput::SendToPeer {
        addr,
        message: UtMetadataMessage::Request { piece },
    }
    ```

- **`MessageReceived { addr, message: Message::Extension { ext_id: UT_METADATA_EXT_ID, payload } }`**
  - Decode `UtMetadataMessage::decode(payload)`.
  - On `Data { piece, total_size, data }`:
    - Validate `piece < pieces.len()` and piece not already received.
    - Store `pieces[piece] = Some(data)`.
    - If all pieces are `Some`: concatenate, SHA1-validate against `info_hash`.
      - On success: emit `MetadataOutput::Done(bytes)`.
      - On failure: discard all pieces, re-request from remaining peers.
  - On `Reject { piece }`: re-request that piece from a different peer.

- **`PeerDisconnected(addr)`**
  - Remove from `self.peers`.
  - Identify which pieces were in-flight to this peer and re-request from remaining peers.

#### SHA1 validation (inside state machine)

```rust
use sha1::{Digest, Sha1};
let hash: [u8; 20] = Sha1::digest(&bytes).into();
if hash != self.info_hash.as_ref() { /* discard & retry */ }
```

`Done` is only emitted after successful validation.

---

### 2. `adapters/peer_io.rs` — MetadataPeerConnector (🚧 needs new type)

`TcpPeerConnector` requires `Arc<Metainfo>` but during metadata fetch we don't have it.
We need a variant that takes only `InfoHash`:

```rust
pub struct MetadataPeerConnector {
    client_id: PeerId,
    info_hash: InfoHash,
}
```

The difference in `connect()` vs the existing `TcpPeerIO::connect()`:

- Use `info_hash` directly instead of `self.metainfo.hash`.
- Send extension handshake with `metadata_size: None` (we don't know it yet).
- No `info_hash` mismatch check against metainfo (already checked on handshake).

The reader task is identical — `Message::read_from` already handles all message types
including `ExtensionHandshake` and `Extension`.

**No reconnect logic** for metadata fetching — if a peer drops, the state machine handles
re-requesting missing pieces from other peers.

---

### 3. `adapters/metadata_io.rs` — Async runner (🚧 empty)

Mirrors `pool_io.rs` but drives `Metadata` instead of `Pool`.

```rust
pub struct MetadataIO {
    info_hash: InfoHash,
    trackers: Vec<String>,
    initial_peers: Vec<SocketAddr>,  // from x.pe in magnet link
    peer_cmds: HashMap<SocketAddr, mpsc::Sender<Message>>,
    peer_events_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    peer_events_rx: mpsc::Receiver<(SocketAddr, PeerEvent)>,
}

impl MetadataIO {
    pub async fn run(
        info_hash: InfoHash,
        trackers: Vec<String>,
        initial_peers: Vec<SocketAddr>,
    ) -> Result<Vec<u8>, Error>
}
```

Main loop:

1. Contact trackers with `info_hash` to collect peer addresses (reuse `TrackerIO`).
   - `TrackerIO` needs a minimal `Metainfo`-like input; simplest: build a stub or extend
     `TrackerIO` to accept just `(info_hash, peer_id, port)`.
2. Seed with `initial_peers` from the magnet link `x.pe`.
3. Connect to peers via `MetadataPeerConnector`.
4. Forward `PeerEvent` → `MetadataInput`, call `Metadata::on_input`, handle outputs:
   - `ConnectPeer(addr)` → spawn new peer connection.
   - `SendToPeer { addr, message }` → encode as `Message::Extension { ext_id, payload }` using peer's ext_id.
   - `Done(bytes)` → stop loop, return `bytes`.

**Note on `SendToPeer`**: `MetadataOutput::SendToPeer` carries a `UtMetadataMessage`.
The adapter must translate it to `Message::Extension { ext_id: peer_ext_id, payload: msg.encode() }`.
The peer's ext_id comes from the state machine (which tracks it per-peer). Either pass it
through `MetadataOutput::SendToPeer` or have the adapter look it up separately.
Simplest: include `ext_id: u8` in `MetadataOutput::SendToPeer`.

---

### 4. `application/magnet.rs` — Entry point (🚧 empty)

```rust
pub async fn fetch_metadata(magnet: MagnetLink) -> Result<Arc<Metainfo>, Error> {
    let peers: Vec<SocketAddr> = magnet.peers
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect();

    let raw_info = MetadataIO::run(magnet.info_hash, magnet.trackers, peers).await?;

    let metainfo = Metainfo::try_from(raw_info.as_slice())
        .map_err(Error::InvalidMetainfo)?;

    Ok(Arc::new(metainfo))
}
```

---

## File status

| File                      | Status      | Notes                                                             |
| ------------------------- | ----------- | ----------------------------------------------------------------- |
| `domain/magnet.rs`        | ✅ Done     | Parser: hex/base32, trackers, peers, error types                  |
| `domain/metadata.rs`      | 🚧 Skeleton | Change `MessageReceived` to carry `Message`; implement `on_input` |
| `adapters/metadata_io.rs` | 🚧 Empty    | Async runner; see section 3                                       |
| `adapters/peer_io.rs`     | 🚧 Extend   | Add `MetadataPeerConnector` (InfoHash-only, no reconnect)         |
| `application/magnet.rs`   | 🚧 Empty    | Entry point; see section 4                                        |
| `src/main.rs`             | 🚧 Todo     | Accept `source: String`, detect magnet vs file                    |
| `application/download.rs` | 🚧 Todo     | Branch on magnet vs torrent bytes before pool                     |

## Modified files

| File                                     | Change                                                      |
| ---------------------------------------- | ----------------------------------------------------------- |
| `src/main.rs`                            | Accept `source: String`, detect magnet vs file              |
| `tortue_lib/src/application/download.rs` | Accept `MagnetLink` or `.torrent` bytes, branch before pool |
| `tortue_lib/src/lib.rs`                  | Export `download_magnet` or unify `download` signature      |
