# Target Architecture

This document describes the long-term structural target for the codebase.
It is independent of the immediate `PieceManager` integration (see `architecture.md`)
and can be approached incrementally.

---

## Principle

The codebase follows a **ports-and-adapters** (hexagonal) layout:

```
           ┌─────────────────────────────────┐
           │           application           │
           │  ┌─────────────────────────┐   │
           │  │         domain          │   │
           │  └─────────────────────────┘   │
           │  ports/ (traits defined here)  │
           └──────────────┬──────────────── ┘
                          │ implements
           ┌──────────────▼──────────────── ┐
           │          adapters               │
           │  tcp / tracker / storage / wire │
           └─────────────────────────────── ┘
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
    torrent.rs                    # Metainfo, InfoHash, piece hashes
    bitfield.rs                   # Bitfield operations
    peer/
      mod.rs                      # PeerId, Handshake (semantic types only)
      message.rs                  # Message enum (no encode/decode here)
    tracker.rs                    # AnnounceRequest, TrackerResponse, Node
    pool.rs                       # peer scheduling, owns PieceManager
    pieces/
      manager.rs                  # block assembly, SHA1 verification
      piece.rs                    # per-piece block state machine

  application/
    download.rs                   # use case: wire ports → adapters, spawn tasks
    ports/
      piece_store.rs              # trait PieceStore
      peer_source.rs              # trait PeerSource

  adapters/
    wire/
      codec.rs                    # bytes ↔ domain::Message (framing + parsing)
      handshake.rs                # bytes ↔ domain::Handshake
    tcp/
      peer_io.rs                  # one TCP connection (uses wire::codec)
      pool_io.rs                  # event loop, drives Pool, calls port impls
    tracker/
      http.rs                     # HTTP tracker → impl PeerSource
      udp.rs                      # UDP tracker → impl PeerSource
    storage/
      disk.rs                     # DiskStorage → impl PieceStore
      # remote.rs                 # future: S3 / HTTP remote → impl PieceStore
      # memory.rs                 # future: in-memory → impl PieceStore (tests)
```

---

## Layer details

### `domain/`

No `tokio`, no `async_trait`, no network types. Only `std`, `sha1`, `rand` for
cryptographic primitives.

**`peer/message.rs`** — keeps the `Message` enum and its semantic variants.
`encode()`, `read_from()`, and `decode_framed()` move to `adapters/wire/codec.rs`.
`AsyncByteReader` moves there too (it is a codec concern, not a domain concept).

**`tracker.rs`** — keeps `AnnounceRequest`, `TrackerResponse`, `Node`.
The `UdpSocket` trait and transport logic move to `adapters/tracker/`.

**`pool.rs`** — peer scheduling, owns `PieceManager` (see `architecture.md`).
Pure state machine: `step(Input) -> Vec<Output>`.

### `application/ports/`

Traits defined by the application that adapters must implement.
No concrete types, no IO. Defined with `async_trait` where needed.

```rust
// application/ports/piece_store.rs
use std::io;

#[async_trait::async_trait]
pub trait PieceStore: Send + Sync {
    async fn write(&mut self, offset: u64, data: &[u8]) -> io::Result<()>;
}
```

```rust
// application/ports/peer_source.rs
use std::net::SocketAddr;
use tokio::sync::mpsc;

// A PeerSource is a background task that emits batches of peer addresses.
// It owns itself and runs until the sender is dropped.
pub trait PeerSource: Send + 'static {
    async fn run(self: Box<Self>, tx: mpsc::Sender<Vec<SocketAddr>>) -> anyhow::Result<()>;
}
```

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
    let metainfo = domain::torrent::decode(torrent_file)?;
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

### `adapters/wire/codec.rs`

All binary encoding and framing for the peer wire protocol.
`Message::encode()` and `Message::decode_framed()` / `Message::read_from()` move here.

```rust
// adapters/wire/codec.rs
pub fn encode(msg: &Message) -> Vec<u8> { ... }
pub async fn read_message<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Message> { ... }
```

`domain::peer::Message` becomes a plain data enum with no encode/decode methods.
The codec is the only code that knows about big-endian framing, message IDs, or buffer sizes.

### `adapters/tracker/`

`http.rs` and `udp.rs` each implement `PeerSource`.
The current `domain/tracker/transport/` (including the `UdpSocket` trait) moves here.

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
domain          ←── application/ports  ←── application/download
                                               │
                                    ┌──────────┴──────────┐
                                    ▼                     ▼
                             adapters/tcp          adapters/tracker
                          (PoolIO, PeerIO)        (http, udp impls)
                                    │
                             adapters/wire
                               (codec.rs)
                                    │
                          adapters/storage
                          (disk, remote, …)
```

`domain` has no arrows pointing outward. Every arrow points left (toward domain).

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

`PoolIO` can be tested by replacing `PeerIO` (TCP) with a fake that sends pre-scripted
`PeerEvent`s into the channel, and `DiskStorage` with a `MemoryStore`. No domain
code changes.

---

## Migration path from current state

The two architectures can coexist during migration. Each step is independent.

| Step  | What changes                                                                                       | Risk                               |
| ----- | -------------------------------------------------------------------------------------------------- | ---------------------------------- |
| **1** | Integrate `PieceManager` into `Pool` (see `architecture.md`)                                       | Medium — core data flow            |
| **2** | Define `PieceStore` trait; make `DiskStorage` implement it; pass `Box<dyn PieceStore>` to `PoolIO` | Low — pure refactor                |
| **3** | Define `PeerSource` trait; make `TrackerIO` implement it                                           | Low — pure refactor                |
| **4** | Move `Message::encode` / `decode_framed` / `read_from` to `adapters/wire/codec.rs`                 | Low — move code, update call sites |
| **5** | Move tracker transport (`UdpSocket`, HTTP/UDP impls) to `adapters/tracker/`                        | Low — move code                    |
| **6** | Rename `ports/disk_storage.rs` → `adapters/storage/disk.rs`                                        | Trivial                            |

Steps 2–6 are mechanical refactors with no behaviour change. They can be done in any order
after step 1. Each step leaves the project in a working state.
