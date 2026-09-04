# Target Architecture

This document describes the structural target for the codebase.

---

## Principle

The codebase follows a **ports-and-adapters** (hexagonal) layout:

```
           ┌─────────────┐
           │   domain    │  ← pure logic, no IO, no async
           └──────┬──────┘
                  │ used by
           ┌──────▼──────┐
           │    ports    │  ← traits only (inside application/)
           └──────┬──────┘
          ┌───────┴────────┐
          ▼                ▼
    application        adapters
    (use cases)    (concrete impls)
```

**Rule**: if the code depends on `tokio`, TCP, or a serialization library → it is
an adapter. If it expresses what something _means_ or _when it is valid_ → domain.

**Dependencies always point inward**: adapters → ports → domain. Domain has no
outward dependencies.

---

## Module tree

```
torrust_lib/src/

  domain/                         # pure logic, no IO, no async
    torrent.rs                    # Metainfo, InfoHash, PieceHash (structs only)
    message.rs                    # Message enum + Debug (no encode/decode)
    peer.rs                       # PeerId
    tracker.rs                    # AnnounceRequest, AnnounceEvent, TrackerResponse, Node
    pool.rs                       # Pool state machine: step(Input) -> Vec<Output>
    bitfield.rs                   # Bitfield operations
    pieces.rs                     # PieceManager, block assembly, SHA1 verification

  application/
    download.rs                   # use case: constructs adapters, wires ports, spawns tasks
    ports/
      piece_store.rs              # trait PieceStore
      peer_source.rs              # trait PeerSource
      peer_connector.rs           # trait PeerConnector + PeerEvent

  adapters/
    bencode.rs                    # Bencode value + encode/decode (pure std)
    torrent_file.rs               # bytes → domain::Metainfo (uses bencode)
    peer_io.rs                    # Handshake, Message codec, PeerIO (TCP), TcpPeerConnector
    pool_io.rs                    # event loop: drives Pool, generic over PieceStore + PeerConnector
    tracker_io.rs                 # TrackerIO: impl PeerSource (HTTP + UDP, uses bencode)
    disk_storage.rs               # DiskStorage: impl PieceStore
```

---

## Layer details

### `domain/`

No `tokio`, no network types. Only `std`, `sha1`, `rand`.

- **`torrent.rs`** — `Metainfo` struct only. Parsing bytes → `Metainfo` is in `adapters/torrent_file.rs`.
- **`message.rs`** — `Message` enum + `Debug`. No encode/decode (those live in `adapters/peer_io.rs`).
- **`peer.rs`** — `PeerId` only. `Handshake` moved to `adapters/peer_io.rs` (wire concern).
- **`tracker.rs`** — `AnnounceRequest`, `AnnounceEvent`, `TrackerResponse`, `Node`. Wire mappings (`as_http_str`, `as_udp_code`) live in `adapters/tracker_io.rs` via `impl AnnounceEvent`.
- **`pool.rs`** — pure state machine. `step(Input) -> Vec<Output>`. Owns `PieceManager`.

### `application/ports/`

Traits that define what the application needs from the outside world.
No concrete types, no IO. Signatures use only `domain` types and `std`.

```rust
// ports/piece_store.rs
pub trait PieceStore: Send {
    async fn write(&mut self, offset: u64, data: &[u8]) -> std::io::Result<()>;
}
```

```rust
// ports/peer_source.rs
pub trait PeerSource: Send + 'static {
    async fn run(self, tx: mpsc::Sender<Vec<SocketAddr>>) -> anyhow::Result<()>;
}
```

```rust
// ports/peer_connector.rs
pub enum PeerEvent {
    Connected(PeerId),
    Disconnected,
    MessageReceived(Message),
}

pub trait PeerConnector: Send + Sync + 'static {
    fn connect(
        &self,
        addr: SocketAddr,
        cmd_rx: mpsc::Receiver<Message>,
        events_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    );
}
```

`PeerEvent` lives in this port so both `pool_io` and `peer_io` can reference it
without coupling to each other.

### `application/download.rs`

Constructs concrete adapters, wires them to ports, spawns tasks. No protocol logic.

```rust
pub async fn download(torrent_file: &[u8], output_dir: PathBuf) -> Result<()> {
    let metainfo = Arc::new(Metainfo::try_from(torrent_file)?);
    let node = Node { id: PeerId::generate("TR", "0.1.0"), port: 1234 };

    let (peers_tx, peers_rx) = mpsc::channel(128);
    for url in &metainfo.announce {
        if let Ok(source) = TrackerIO::new(url, Arc::clone(&metainfo), node) {
            let tx = peers_tx.clone();
            tokio::spawn(async move { source.run(tx).await });
        }
    }

    let connector = TcpPeerConnector::new(node.id, Arc::clone(&metainfo));
    let storage = DiskStorage::new(&metainfo, output_dir).await?;
    let mut pool = PoolIO::new(Arc::clone(&metainfo), peers_rx, connector, storage);
    tokio::spawn(async move { pool.run().await });

    shutdown_signal().await;
    Ok(())
}
```

### `adapters/peer_io.rs`

Contains three concerns, all tightly coupled to the TCP peer wire protocol:

- **`Handshake`** struct + `encode`/`decode` methods (wire format, not domain)
- **`impl Message`** — `encode()`, `read_from()`, `decode()` (framing + parsing)
- **`PeerIO`** — one TCP connection (connect, reconnect, handshake, read/write loop)
- **`TcpPeerConnector`** — `impl PeerConnector`: spawns a `PeerIO` task per peer

`TcpPeerConnector::connect` receives `events_tx: Sender<(SocketAddr, PeerEvent)>` and
passes it directly to `PeerIO`. No intermediate channel, no forwarding task.

### `adapters/pool_io.rs`

Event loop. Generic over `S: PieceStore` and `C: PeerConnector`.

```rust
pub struct PoolIO<S, C> { ... }

impl<S: PieceStore, C: PeerConnector> PoolIO<S, C> {
    pub fn new(metainfo, peers_rx, peer_connector: C, piece_store: S) -> Self
}
```

Drives `Pool` (domain) by translating `PeerEvent`s into `pool::Input` and dispatching
`pool::Output`. `spawn_peer` delegates entirely to `self.peer_connector.connect(...)` —
never imports `PeerIO` directly.

### `adapters/tracker_io.rs`

`impl PeerSource for TrackerIO`. HTTP and UDP tracker protocols in a single file,
organized with section banners. Wire format mappings (`impl AnnounceEvent`,
`impl Transport`) live here. `TrackerIO` has no `peers_tx` field — the channel is
passed via `PeerSource::run(self, tx)`.

### `adapters/disk_storage.rs`

`impl PieceStore for DiskStorage`. Handles single-file and multi-file torrents.

---

## Observability (progress bar / TUI)

`Pool` exposes a pure `snapshot() -> PoolSnapshot` method that captures the current
state of the download. `PoolIO` publishes it after every `pool.step()` via a
`watch::Sender<PoolSnapshot>`. Callers receive a `watch::Receiver<PoolSnapshot>` and
can redraw a progress bar or a full TUI on every change.

```rust
// domain/pool.rs
pub struct PoolSnapshot {
    pub pieces_total: usize,
    pub pieces_done: usize,
    pub pieces_in_flight: usize,
    pub bytes_downloaded: u64,
    pub download_speed_bps: f64,    // computed in PoolIO from WritePiece timestamps
    pub peers: Vec<PeerInfo>,
}

pub struct PeerInfo {
    pub addr: SocketAddr,
    pub peer_id: Option<PeerId>,    // set after handshake (already in PeerState)
    pub in_flight: usize,
    pub is_choking: bool,
}
```

`download_speed_bps` is computed in `PoolIO` (not domain) — it requires wall-clock
time which the domain does not have. `PoolIO` tracks `(bytes_written, Instant)` at
each `Output::WritePiece`.

**Simple progress bar** (`indicatif`):
```rust
let (fut, progress) = download(&data, out);
tokio::spawn(async move {
    let bar = ProgressBar::new(100);
    while progress.changed().await.is_ok() {
        let s = progress.borrow();
        bar.set_position(s.pieces_done as u64 * 100 / s.pieces_total as u64);
    }
    bar.finish();
});
fut.await?;
```

**Full TUI** (`ratatui`): same `watch::Receiver<PoolSnapshot>` — redraw each frame
using `pieces`, `peers`, `download_speed_bps`, etc. No additional plumbing needed.

---

## Dependency graph

```
domain  ←──  application/ports  ←──────────────────────────┐
                    ↑                                        │
             application/download                           │
                    │                                        │
         ┌──────────┼──────────────┐                        │
         ▼          ▼              ▼                        │
     pool_io    tracker_io    disk_storage               peer_io
  (PoolIO<S,C>) (PeerSource)  (PieceStore)         (TcpPeerConnector)
```

`domain` has no outward arrows. `application/ports` depends only on `domain`.
Adapters depend on ports. `pool_io` uses `PeerConnector` (port), never `PeerIO` directly.

---

## Swap examples

### Disk storage → remote storage

```rust
// Only the call site changes
let storage = RemoteStorage::new("https://storage.example.com", &metainfo);
let mut pool = PoolIO::new(metainfo, peers_rx, connector, storage);
```

### UDP tracker → DHT

```rust
pub struct DhtSource { ... }
impl PeerSource for DhtSource { ... }

tokio::spawn(async move { DhtSource::new(metainfo).run(peers_tx).await });
```

### Real TCP → in-memory (tests)

```rust
struct FakePeerConnector { script: Vec<PeerEvent> }

impl PeerConnector for FakePeerConnector {
    fn connect(&self, addr, _cmd_rx, events_tx) {
        let events = self.script.clone();
        tokio::spawn(async move {
            for event in events {
                events_tx.send((addr, event)).await.unwrap();
            }
        });
    }
}

// PoolIO is fully testable with no TCP sockets
let pool = PoolIO::new(metainfo, peers_rx, FakePeerConnector { script }, MemoryStore::new());
```
