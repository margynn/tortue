# Architecture

## Current state

```
application::download
  ├── TrackerIO          (adapter) — UDP/HTTP tracker, emits Vec<SocketAddr>
  └── PoolIO             (adapter) — event loop, owns peer tasks
        ├── Pool         (domain)  — peer scheduling, in-flight tracking
        └── PeerIO×N    (adapter) — one TCP connection per peer
```

`PieceManager` exists in `domain/pieces/manager.rs` but is **never instantiated**.
`pool::Input::PieceVerified` exists but is **never produced** by `PoolIO`.

### Identified problems

#### 1. Duplicated block/piece logic between Pool and PieceManager

`Pool` (domain/pool.rs) owns:

```rust
const BLOCK_SIZE: u32 = 16_384;

fn piece_blocks(&self, piece: usize) -> Vec<(u32, u32)>
```

`PieceManager` (domain/pieces/manager.rs) owns:

```rust
use super::piece::BLOCK_SIZE;

pub fn missing_blocks(&self, piece: u32) -> impl Iterator<Item = BlockRange>
pub fn mark_block_requested(&mut self, piece: u32, begin: usize)
```

`Pool.schedule_requests()` uses its own `piece_blocks()` — which returns **all** blocks of a
piece — to build `Request` messages, while ignoring `PieceManager.missing_blocks()` which
tracks which blocks are actually still needed. This means Pool can re-request blocks already
received or in progress at the piece layer.

#### 2. PieceManager has an imperative API in an event-driven codebase

`Pool` exposes `step(input: Input) -> Vec<Output>` — pure, event-driven.
`PieceManager` exposes direct mutable methods: `receive_block`, `mark_block_requested`,
`storage_completed`. This asymmetry complicates integration and testing.

#### 3. Block data is discarded at Pool level

When `Pool.on_message()` matches `Message::Piece { index, begin, .. }`, the block data is
silently dropped. `Pool` updates its in-flight counter and reschedules requests, but the
actual bytes never reach `PieceManager`. No piece is ever assembled or verified.

#### 4. Storage is never written

`PieceManager.receive_block()` emits `PieceEvent::PieceCompleted { command: StorageCommand::Write { .. } }`,
but nothing in `PoolIO` calls `PieceManager`, so no `StorageCommand` is ever produced and
`ports::disk_storage` is never invoked.

---

## Design options

### Option A — PieceManager inside Pool

`Pool` owns `PieceManager`. It becomes a unified download coordinator: it schedules peer
requests **and** assembles/verifies pieces.

```
PoolIO
  └── Pool  ←─ owns ──► PieceManager
```

**Data flow**

```
PeerIO ──► PeerEvent::MessageReceived(Piece { index, begin, block })
              │
              ▼
           PoolIO
              │  pool::Input::MessageReceived { addr, message }
              ▼
           Pool.step()
              │  internally: piece_manager.receive_block(index, begin, block)
              │
              ├─ PieceEvent::BlockAcknowledged  →  reschedule requests
              ├─ PieceEvent::PieceCompleted     →  Output::WritePiece + Output::PieceVerified (internal)
              └─ PieceEvent::PieceInvalid       →  reset, reschedule
              │
              ▼
           Pool::Output
              ├── ConnectPeer(addr)
              ├── SendToPeer { addr, message }
              ├── WritePiece { offset, data }      ← new
              └── Completed
```

`Pool.schedule_requests()` replaces `piece_blocks()` with `piece_manager.missing_blocks()`
so it only requests blocks that are genuinely missing (not yet received or in-flight).

`PoolIO` handles `Output::WritePiece` by calling the storage port. It feeds
`pool::Input::PieceVerified` after the write completes (or immediately if write is async
fire-and-forget).

**Advantages**
- `PoolIO` stays simple: one domain object to drive, one event loop.
- No inter-domain channel needed; the coupling between scheduling and assembly is
  synchronous and co-located.
- `Pool.schedule_requests()` can use the real missing-block state from `PieceManager`
  instead of recomputing all blocks.
- `BLOCK_SIZE` and `piece_blocks()` are removed from `Pool`; they live only in
  `PieceManager`/`Piece`.

**Drawbacks**
- `Pool` now has two responsibilities: peer scheduling and piece assembly. Its test surface
  grows.
- `PieceManager` can still be unit-tested in isolation, but only by constructing a `Pool` or
  by keeping a public API on `PieceManager` that tests call directly.

---

### Option B — PieceManager outside Pool, with Input/Output enums

Both `Pool` and `PieceManager` are pure event-driven state machines. `PoolIO` orchestrates
both, routing outputs from one as inputs to the other.

```
PoolIO
  ├── Pool          step(Input) -> Vec<Output>
  └── PieceManager  step(Input) -> Vec<Output>
```

**PieceManager enums**

```rust
pub enum Input {
    BlockReceived { piece: u32, offset: usize, data: Vec<u8> },
    StorageCompleted { piece: u32 },
}

pub enum Output {
    BlockAcknowledged,
    PieceCompleted { piece: u32, offset: u64, data: Vec<u8> },
    PieceInvalid { piece: u32 },
    NeedBlocks { piece: u32, blocks: Vec<BlockRange> },  // optional: push model
}
```

**Pool changes**

`Pool` no longer handles `Message::Piece` block data. It receives a dedicated input:

```rust
pub enum Input {
    PeersDiscovered(Vec<SocketAddr>),
    PeerConnected { addr: SocketAddr, peer_id: PeerId },
    PeerDisconnected(SocketAddr),
    MessageReceived { addr: SocketAddr, message: Message },
    BlockAcknowledged { piece: u32, begin: u32 },  // replaces PieceVerified for in-flight
    PieceVerified(usize),
}
```

**Data flow in PoolIO**

```
PeerEvent::MessageReceived(Piece { index, begin, block })
  │
  ├─► pool::Input::BlockAcknowledged { piece: index, begin }
  │       Pool updates in-flight, schedules next requests
  │
  └─► piece_manager::Input::BlockReceived { piece: index, offset: begin, data: block }
          │
          ├─ Output::BlockAcknowledged          → (no-op or metrics)
          ├─ Output::PieceCompleted { .. }      → write to storage
          │                                        then pool::Input::PieceVerified
          └─ Output::PieceInvalid { piece }     → pool::Input::PieceVerified(piece)?
                                                   or a dedicated InvalidPiece input
                                                   to trigger re-request
```

`Pool.schedule_requests()` cannot call `piece_manager.missing_blocks()` directly (they are
separate). Two approaches:

1. **Pull**: `PoolIO` queries `piece_manager.missing_blocks(piece)` before calling
   `pool.schedule_requests()`, and passes the result in via an enriched input.
2. **Push**: `PieceManager` emits `Output::NeedBlocks` after a reset; `PoolIO` feeds this
   back to Pool as a `BlocksNeeded` input.

The simplest is to keep Pool scheduling all blocks and deduplicate at the `PieceManager`
level: `PieceManager.receive_block()` is idempotent for already-received blocks (already the
case in `Piece.receive_block()`).

**Advantages**
- Perfect separation: `Pool` is purely about peer topology and scheduling; `PieceManager`
  is purely about data integrity.
- Each can be unit-tested with zero coupling to the other.
- `PieceManager` is reusable in a seeding scenario without dragging in Pool logic.

**Drawbacks**
- `PoolIO` becomes a non-trivial router: it must split `Message::Piece` across two state
  machines and route outputs back as inputs.
- The "missing blocks" problem requires an explicit solution (pull or push model above).
- More enum variants to maintain across two domain modules.

---

## Recommendation

**Option A** is the right fit for the current scope.

The coupling between peer scheduling and piece state is inherent to the BitTorrent
protocol: the decision of *what to request next* depends directly on *what has already been
received*. Placing `PieceManager` inside `Pool` makes this dependency explicit and
synchronous, removes the duplicated `BLOCK_SIZE`/`piece_blocks` logic, and keeps `PoolIO`
as a thin adapter with a single domain object to drive.

`PieceManager` remains a distinct struct with its own unit tests. The boundary is internal
to `Pool`, not across an async channel.

Option B becomes attractive if `PieceManager` needs to be reused (e.g. seeding, resuming
from disk) in a context where `Pool` is not present. At that point, adding `Input/Output`
enums to `PieceManager` and extracting it back out of `Pool` is a contained refactor.

---

## Target architecture (Option A)

```
application::download
  ├── TrackerIO                    (adapter)
  └── PoolIO                       (adapter)
        │  drives via Input/Output
        └── Pool                   (domain) — owns PieceManager
              ├── PeerState×N
              ├── PieceManager
              │     └── Piece×N
              └── Bitfield (needed)

Pool::Output
  ├── ConnectPeer(SocketAddr)
  ├── SendToPeer { addr, message }
  ├── WritePiece { offset: u64, data: Vec<u8> }
  └── Completed

PoolIO responsibilities
  ├── spawn/drop PeerIO tasks
  ├── forward Pool::Output::SendToPeer → peer cmd channel
  ├── forward Pool::Output::WritePiece → disk_storage port
  └── feed Pool::Input::PieceVerified after write
```

### Changes required

| Location | Change |
|---|---|
| `domain/pool.rs` | Own `PieceManager`; handle `Message::Piece` data through it; remove `piece_blocks()` and local `BLOCK_SIZE`; add `Output::WritePiece` |
| `domain/pieces/manager.rs` | Add `reset_requested_block()`; keep imperative API (internal to Pool) |
| `adapters/pool_io.rs` | Add `DiskStorage`; handle `Output::WritePiece` → write + feed `Input::PieceVerified`; remove dead `verified_pieces` counter |
| `application/download.rs` | Construct `DiskStorage` and pass it to `PoolIO` |

---

## Implementation details

### 1. `domain/pieces/manager.rs`

No structural change needed — `PieceManager` keeps its imperative API since it is now
internal to `Pool` (no async channel between them). One new method is required to support
the disconnect case (see Pool section):

```rust
/// Called when a peer disconnect makes an in-flight block unserviceable.
/// Resets the block back to Missing so schedule_requests() will re-request it.
pub fn reset_requested_block(&mut self, piece: u32, begin: usize) {
    let index = begin / BLOCK_SIZE;
    let p = &mut self.pieces[piece as usize];
    if matches!(p.blocks[index], BlockState::Requested { .. }) {
        p.blocks[index] = BlockState::Missing;
    }
}
```

`missing_blocks()` already filters for `BlockState::Missing` only, so it naturally excludes
both in-flight (`Requested`) and done (`Received`) blocks. `schedule_requests()` can use it
directly without a redundant `in_flight.contains_key()` check — **provided** the two systems
are kept in sync (see Pool section below).

---

### 2. `domain/pool.rs`

#### Struct

```rust
pub struct Pool {
    metainfo: Metainfo,
    needed: Bitfield,
    peers: HashMap<SocketAddr, PeerState>,
    availability: HashMap<usize, HashSet<SocketAddr>>,
    in_flight: HashMap<(usize, u32), SocketAddr>,  // (piece, begin) -> peer
    pieces: PieceManager,                           // ← new
}

impl Pool {
    pub fn new(metainfo: Metainfo) -> Self {
        let pieces = PieceManager::new(metainfo.clone());
        // ...
        Self { .., pieces }
    }
}
```

Remove `const BLOCK_SIZE` and `fn piece_blocks()`.
Remove `Input::PieceVerified` — piece verification is now internal, triggered from
`on_block_received`. `PieceVerified` becomes a private method call, not a public input.

Keep `Input::PieceVerified` only to close the async loop-back from `PoolIO` after the write
completes (see PoolIO section). Rename it for clarity:

```rust
pub enum Input {
    PeersDiscovered(Vec<SocketAddr>),
    PeerConnected { addr: SocketAddr, peer_id: PeerId },
    PeerDisconnected(SocketAddr),
    MessageReceived { addr: SocketAddr, message: Message },
    WriteCompleted(usize),  // piece index — fed back by PoolIO after disk write
}
```

#### Output

```rust
pub enum Output {
    ConnectPeer(SocketAddr),
    SendToPeer { addr: SocketAddr, message: Message },
    WritePiece { piece: usize, offset: u64, data: Vec<u8> },  // ← new
    Completed,
}
```

#### `on_message` — handling `Message::Piece`

```rust
Message::Piece { index, begin, block } => {
    // 1. In-flight accounting (peer responsibility)
    if self.in_flight.remove(&(*index as usize, *begin)).is_some() {
        if let Some(state) = self.peers.get_mut(&addr) {
            state.in_flight = state.in_flight.saturating_sub(1);
        }
    }

    // 2. Piece assembly and verification (piece manager responsibility)
    let mut outputs = vec![];
    match self.pieces.receive_block(*index, *begin as usize, block.clone()) {
        Ok(PieceEvent::BlockReceived) => {},
        Ok(PieceEvent::PieceCompleted { piece, command: StorageCommand::Write { offset, data } }) => {
            outputs.push(Output::WritePiece { piece: piece as usize, offset, data });
        },
        Ok(PieceEvent::PieceInvalid { piece }) => {
            // PieceManager already reset the piece internally.
            // Re-add to needed so schedule_requests() will retry.
            let _ = self.needed.set_bit(piece as usize);
        },
        Err(_) => {},
    }

    outputs.extend(self.schedule_requests());
    outputs
}
```

#### `on_disconnected` — reset orphaned in-flight blocks

When a peer disconnects, its in-flight blocks are removed from `self.in_flight`. But
`PieceManager` still has those blocks as `Requested`. Without a reset they will never appear
in `missing_blocks()` and will never be re-requested.

```rust
fn on_disconnected(&mut self, addr: SocketAddr) -> Vec<Output> {
    self.peers.remove(&addr);
    self.availability.retain(|_, peers| {
        peers.remove(&addr);
        !peers.is_empty()
    });

    // Reset orphaned in-flight blocks in PieceManager before removing them.
    let orphaned: Vec<(usize, u32)> = self.in_flight
        .iter()
        .filter(|(_, peer)| *peer == addr)
        .map(|(key, _)| *key)
        .collect();

    for (piece, begin) in orphaned {
        self.in_flight.remove(&(piece, begin));
        self.pieces.reset_requested_block(piece as u32, begin as usize);
    }

    self.schedule_requests()
}
```

#### `on_write_completed` (replaces `on_piece_verified`)

```rust
fn on_write_completed(&mut self, piece: usize) -> Vec<Output> {
    self.pieces.storage_completed(piece as u32);  // sets the bitfield
    let _ = self.needed.unset_bit(piece);

    if self.needed.into_iter().next().is_none() {
        return vec![Output::Completed];
    }

    self.schedule_requests()
}
```

#### `schedule_requests` — use `PieceManager::missing_blocks`

`missing_blocks()` returns only `BlockState::Missing` blocks. Since `on_disconnected` resets
orphaned `Requested` blocks back to `Missing`, and since `mark_block_requested` is called
every time we emit a `Request` message, the two systems stay in sync.

```rust
fn schedule_requests(&mut self) -> Vec<Output> {
    let mut outputs = vec![];
    let mut rng = rand::rng();

    let needed: Vec<usize> = self.needed.into_iter().map(|i| i as usize).collect();
    // Rarest first
    let mut needed = needed;
    needed.sort_by_key(|&piece| {
        self.availability.get(&piece).map_or(usize::MAX, |peers| peers.len())
    });

    for piece in needed {
        let peer_addrs: Vec<SocketAddr> = match self.availability.get(&piece) {
            Some(peers) => peers.iter().copied().collect(),
            None => continue,
        };

        // Use PieceManager's missing_blocks — only truly unrequested blocks.
        for BlockRange { begin, length } in self.pieces.missing_blocks(piece as u32) {
            let begin = begin as u32;
            let length = length as u32;

            // in_flight is still needed for peer accounting (which peer holds the block).
            if self.in_flight.contains_key(&(piece, begin)) {
                continue;
            }

            let candidate = peer_addrs
                .iter()
                .filter(|addr| {
                    self.peers.get(addr).map_or(false, |s| {
                        !s.peer_choking && s.in_flight < MAX_IN_FLIGHT_PER_PEER
                    })
                })
                .choose(&mut rng)
                .copied();

            if let Some(addr) = candidate {
                self.peers.get_mut(&addr).unwrap().in_flight += 1;
                self.in_flight.insert((piece, begin), addr);
                self.pieces.mark_block_requested(piece as u32, begin as usize);

                outputs.push(Output::SendToPeer {
                    addr,
                    message: Message::Request { index: piece as u32, begin, length },
                });
            }
        }
    }

    outputs
}
```

> `in_flight` remains necessary even with `missing_blocks()` because it maps `(piece, begin)`
> to the specific peer — information that `PieceManager` does not have. It is used for
> per-peer in-flight counting and for the disconnect cleanup.

---

### 3. `adapters/pool_io.rs`

#### Struct

```rust
pub struct PoolIO {
    metainfo: Metainfo,
    client_id: PeerId,
    peers_rx: mpsc::Receiver<Vec<SocketAddr>>,
    peer_cmds: HashMap<SocketAddr, mpsc::Sender<peer::Message>>,
    pool_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    pool_rx: mpsc::Receiver<(SocketAddr, PeerEvent)>,
    storage: DiskStorage,  // ← new (replaces verified_pieces counter)
}
```

`DiskStorage` is constructed outside and passed in, or constructed inside `new()` after an
async init step. Simplest: take it as a parameter from `application::download`.

#### Event loop — handling `WritePiece`

The write is async. After it completes, `Pool` must receive `Input::WriteCompleted` to
update its `needed` bitfield and trigger the `Completed` sentinel. The cleanest way is to
handle it inline, immediately re-entering the pool step:

```rust
for out in pool.step(input) {
    match out {
        pool::Output::WritePiece { piece, offset, data } => {
            match self.storage.write(offset, &data).await {
                Ok(()) => {
                    // Feed WriteCompleted back synchronously before next select!
                    for out2 in pool.step(pool::Input::WriteCompleted(piece)) {
                        self.handle_output(out2).await;
                    }
                },
                Err(e) => tracing::error!(piece, error = %e, "disk write failed"),
            }
        },
        other => self.handle_output(other).await,
    }
}
```

This keeps the event loop single-threaded and avoids introducing a second channel for write
completions. The nested `pool.step()` call is safe because `Pool` is not borrowed elsewhere.

> If write latency becomes a concern, writes can be moved to a dedicated task with a
> completion channel feeding `pool_rx`. That is an incremental change that does not alter
> the domain model.

#### Remove dead code

- Remove `verified_pieces: usize` field and its log statement.
- Remove the `pool::Input::PieceVerified(_)` arm in the pre-step match (the counter was the
  only thing it did).

---

### 4. `application/download.rs`

`DiskStorage::new` is async and fallible, so it must be awaited before constructing `PoolIO`:

```rust
pub async fn download(torrent_file: &[u8], output_dir: PathBuf) -> Result<()> {
    let metainfo = domain::torrent::decode(torrent_file)?;
    // ...
    let storage = DiskStorage::new(&metainfo, output_dir).await?;
    let mut pool_runner = PoolIO::new(metainfo.clone(), node.id, peers_rx, storage);
    tokio::spawn(async move { pool_runner.run().await });
    // ...
}
```

---

### Invariants to maintain

| Invariant | Enforced by |
|---|---|
| A block is in `in_flight` iff `PieceManager` has it as `Requested` | `schedule_requests` always calls both `in_flight.insert` and `mark_block_requested` together |
| On disconnect, orphaned `Requested` blocks return to `Missing` | `on_disconnected` resets them before clearing `in_flight` |
| `needed` bitfield and `PieceManager` bitfield stay in sync | `on_write_completed` calls both `needed.unset_bit` and `pieces.storage_completed` |
| A piece is re-queued for retry after hash failure | `on_message` calls `needed.set_bit` when `PieceInvalid` is returned |
