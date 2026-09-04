# Target Architecture

This document describes the long-term structural target for the codebase.
It is independent of the immediate `PieceManager` integration (see `architecture.md`)
and can be approached incrementally.

---

## Principle

The codebase follows a **ports-and-adapters** (hexagonal) layout:

```
           ┌─────────────┐
           │   domain    │
           └──────┬──────┘
                  │ used by
           ┌──────▼──────┐
           │    ports    │  ← traits only, no IO
           └──────┬──────┘
          ┌───────┴────────┐
          ▼                ▼
    application        adapters
    (use cases)    (concrete impls)
```

**Rule**: if the code depends on `tokio`, `bytes`, TCP, or a serialization library
→ it is an adapter. If it expresses what something _means_ or _when it is valid_
→ it is domain or application.

**Dependencies always point inward**: adapters depend on ports, ports are defined
by the application, domain has no outward dependencies.

Swapping an adapter (e.g. disk storage → remote storage) requires only providing a
new impl of the relevant port trait. The domain and application layers are untouched.

---

## Target module tree

```
torrust_lib/src/

  domain/                         # pure logic, no IO, no async runtime deps
    torrent.rs                    # Metainfo, InfoHash, piece hashes (structs only, no parsing)
    bitfield.rs                   # Bitfield operations
    peer/
      mod.rs                      # PeerId, Handshake (semantic types only)
      message.rs                  # Message enum (no encode/decode here)
    tracker.rs                    # AnnounceRequest, TrackerResponse, Node
    pool.rs                       # peer scheduling, owns PieceManager
    pieces/
      manager.rs                  # block assembly, SHA1 verification
      piece.rs                    # per-piece block state machine

  ports/
    piece_store.rs                # trait PieceStore
    peer_source.rs                # trait PeerSource
    peer_connector.rs             # trait PeerConnector + PeerEvent

  application/
    download.rs                   # use case: wire ports → adapters, spawn tasks

  adapters/
    bencode.rs                    # Bencode value + encode/decode (single file, pure std)
    torrent_file.rs               # bytes → domain::Metainfo (uses bencode)
    peer_io.rs                    # one TCP connection: bytes ↔ domain::Message
    pool_io.rs                    # composition root: event loop + wires PeerIO onto Pool
    tracker_io.rs                 # impl PeerSource (HTTP/UDP, uses bencode)
    storage/
      disk.rs                     # DiskStorage → impl PieceStore
      # remote.rs                 # future: S3 / HTTP remote → impl PieceStore
      # memory.rs                 # future: in-memory → impl PieceStore (tests)
```

---

## Layer details

### `domain/`

No `tokio`, no `async_trait`, no network types. Only `std`, `sha1`, `rand` for
cryptographic primitives. No serialization: `domain/torrent.rs` holds the
`Metainfo` struct but never parses bytes — that is `adapters/torrent_file.rs`.

**`peer/message.rs`** — keeps the `Message` enum and its semantic variants.
`encode()`, `read_from()`, and `decode_framed()` move to `adapters/peer_io.rs`.
`AsyncByteReader` moves there too (it is a codec concern, not a domain concept).

**`tracker.rs`** — keeps `AnnounceRequest`, `TrackerResponse`, `Node`.
The `UdpSocket` trait and transport logic move to `adapters/tracker_io.rs`.

**`pool.rs`** — peer scheduling, owns `PieceManager` (see `architecture.md`).
Pure state machine: `step(Input) -> Vec<Output>`.

### `ports/`

Traits that define what the application needs from the outside world.
No concrete types, no IO. Only `domain` types and `std` in signatures.

```rust
// ports/piece_store.rs
use std::io;

#[async_trait::async_trait]
pub trait PieceStore: Send + Sync {
    async fn write(&mut self, offset: u64, data: &[u8]) -> io::Result<()>;
}
```

```rust
// ports/peer_source.rs
use std::net::SocketAddr;
use tokio::sync::mpsc;

// A PeerSource is a background task that emits batches of peer addresses.
// It owns itself and runs until the sender is dropped.
pub trait PeerSource: Send + 'static {
    async fn run(self: Box<Self>, tx: mpsc::Sender<Vec<SocketAddr>>) -> anyhow::Result<()>;
}
```

```rust
// ports/peer_connector.rs
use std::net::SocketAddr;
use tokio::sync::mpsc;

use crate::domain::peer::{Message, PeerId};

pub enum PeerEvent {
    Connected(PeerId),
    Disconnected,
    MessageReceived(Message),
}

// Spawns a background task that manages one peer connection.
// The task sends PeerEvents inbound and receives Messages outbound.
// Dropping cmd_tx signals the task to stop.
pub trait PeerConnector: Send + Sync + 'static {
    fn connect(
        &self,
        addr: SocketAddr,
        cmd_rx: mpsc::Receiver<Message>,
        events_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    );
}
```

With this port, `pool_io` only holds a `Box<dyn PeerConnector>` and never imports
`PeerIO`. Swapping TCP for an in-memory fake requires only a new impl:

```rust
// tests
struct FakePeerConnector { script: Vec<PeerEvent> }

impl PeerConnector for FakePeerConnector {
    fn connect(&self, addr: SocketAddr, _cmd_rx: Receiver<Message>, events_tx: Sender<...>) {
        let events = self.script.clone();
        tokio::spawn(async move {
            for event in events {
                events_tx.send((addr, event)).await.unwrap();
            }
        });
    }
}
```

`PeerEvent` moves from `adapters/peer_io.rs` to this port so both `pool_io` and
`peer_io` can reference it without a circular dependency.

### `application/download.rs`

The use case. It constructs adapters, wires them to ports, and spawns tasks.
It does not contain any protocol logic.

```rust
pub async fn download(
    torrent_file: &[u8],
    output_dir: PathBuf,
    // Callers inject concrete adapters — the use case only sees the trait.
    store: impl PieceStore + 'static,
) -> anyhow::Result<()> {
    let metainfo = adapters::torrent_file::parse(torrent_file)?;
    let node = Node { id: PeerId::generate("TR", "0.1.0"), port: 1234 };

    let (peers_tx, peers_rx) = mpsc::channel(128);

    for url in &metainfo.announce {
        if let Ok(source) = tracker::from_url(url, metainfo.clone(), node) {
            let tx = peers_tx.clone();
            tokio::spawn(async move { source.run(tx).await });
        }
    }

    let mut pool_runner = PoolIO::new(metainfo.clone(), node.id, peers_rx, store);
    pool_runner.run().await
}
```

The caller (binary / integration test) decides which store to use:

```rust
// production
let store = DiskStorage::new(&metainfo, output_dir).await?;
download(file, dir, store).await?;

// test / remote
let store = MemoryStore::new(&metainfo);
download(file, dir, store).await?;
```

### `adapters/peer_io.rs`

One TCP connection: codec (bytes ↔ `domain::Message`) + socket lifecycle (connect,
reconnect, handshake). `domain::peer::Message` becomes a plain data enum with no
encode/decode methods. The codec is the only code that knows about big-endian
framing, message IDs, or buffer sizes.

### `adapters/pool_io.rs`

Event loop. Drives `Pool` (domain) by translating `PeerEvent`s into `pool::Input`
and dispatching `pool::Output`. Holds a `Box<dyn PeerConnector>` — never imports
`PeerIO` directly.

### `adapters/tracker_io.rs`

Implements `PeerSource`. Handles HTTP and UDP tracker protocols, parses bencode
responses into `Vec<SocketAddr>`.

```rust
// adapters/tracker/udp.rs
pub struct UdpTrackerSource { url: Url, metainfo: Metainfo, node: Node }

impl PeerSource for UdpTrackerSource {
    async fn run(self: Box<Self>, tx: mpsc::Sender<Vec<SocketAddr>>) -> anyhow::Result<()> {
        // announce loop, parse compact peers, send to tx
    }
}
```

### `adapters/storage/`

`disk.rs` is the current `ports/disk_storage.rs`, renamed and made to implement `PieceStore`.

```rust
// adapters/storage/disk.rs
pub struct DiskStorage { files: Vec<OutputFile> }

#[async_trait]
impl PieceStore for DiskStorage {
    async fn write(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        // existing write logic
    }
}
```

Adding a remote store requires only a new file:

```rust
// adapters/storage/remote.rs
pub struct RemoteStorage { endpoint: Url, client: reqwest::Client }

#[async_trait]
impl PieceStore for RemoteStorage {
    async fn write(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        // PUT to remote endpoint
    }
}
```

No domain or application code changes.

---

## Dependency graph

```
domain  ←──  ports  ←──────────────────────────────────────┐
               ↑                                            │
               │ depends on                                 │
         application/download                               │
               │                                            │
               └──────────┬──────────────┐                  │
                          ▼              ▼                  │
                      pool_io       tracker_io          storage/disk
                          │         (impl               (impl
                          ▼          PeerSource)         PieceStore)
                      peer_io
                   (impl PeerConnector)
```

`domain` has no arrows pointing outward. `ports` depends only on `domain`.
Adapters depend on `ports`, never on each other (except `pool_io` → `peer_io`
which is the composition root).

---

## Swap examples

### Disk storage → remote storage

```rust
// Before
let store = DiskStorage::new(&metainfo, output_dir).await?;

// After — only the call site changes
let store = RemoteStorage::new("https://storage.example.com", &metainfo);

download(torrent_file, store).await?;
```

### UDP tracker → DHT

```rust
// Implement PeerSource for a DHT node
pub struct DhtSource { ... }
impl PeerSource for DhtSource { ... }

// Plug it in at the call site
tokio::spawn(async move { DhtSource::new(metainfo).run(peers_tx).await });
```

### Real TCP → in-memory transport (tests)

Because `pool_io` holds a `Box<dyn PeerConnector>`, the TCP transport is fully
swappable. A test injects a `FakePeerConnector` that replays a scripted sequence of
`PeerEvent`s without opening any socket. Combined with a `MemoryStore` for
`PieceStore`, `PoolIO` is fully unit-testable with no IO.

---

## Migration path from current state

The two architectures can coexist during migration. Each step is independent.

| Step  | What changes                                                                                           | Risk                               |
| ----- | ------------------------------------------------------------------------------------------------------ | ---------------------------------- |
| **1** | Integrate `PieceManager` into `Pool` (see `architecture.md`)                                           | Medium — core data flow            |
| **2** | Define `PieceStore` trait; make `DiskStorage` implement it; pass `Box<dyn PieceStore>` to `PoolIO`     | Low — pure refactor                |
| **3** | Define `PeerSource` trait; make `TrackerIO` implement it                                               | Low — pure refactor                |
| **4** | Define `PeerConnector` + `PeerEvent` port; make `PeerIO` implement it; inject into `PoolIO`            | Low — pure refactor                |
| **5** | Move `Message::encode` / `decode_framed` / `read_from` into `adapters/peer_io.rs`                     | Low — move code, update call sites |
| **6** | Move `bencode` to `adapters/bencode.rs`; add `adapters/torrent_file.rs` for metainfo parsing          | Low — move code                    |
| **7** | Move tracker transport (`UdpSocket`, HTTP/UDP impls) into `adapters/tracker_io.rs`                    | Low — move code                    |

Each step leaves the project in a working state.
