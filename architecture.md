# Architecture

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

| Location                   | Change                                                                                                                                |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `domain/pool.rs`           | Own `PieceManager`; handle `Message::Piece` data through it; remove `piece_blocks()` and local `BLOCK_SIZE`; add `Output::WritePiece` |
| `domain/pieces/manager.rs` | Add `reset_requested_block()`; keep imperative API (internal to Pool)                                                                 |
| `adapters/pool_io.rs`      | Add `DiskStorage`; handle `Output::WritePiece` → write + feed `Input::PieceVerified`; remove dead `verified_pieces` counter           |
| `application/download.rs`  | Construct `DiskStorage` and pass it to `PoolIO`                                                                                       |

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

| Invariant                                                          | Enforced by                                                                                  |
| ------------------------------------------------------------------ | -------------------------------------------------------------------------------------------- |
| A block is in `in_flight` iff `PieceManager` has it as `Requested` | `schedule_requests` always calls both `in_flight.insert` and `mark_block_requested` together |
| On disconnect, orphaned `Requested` blocks return to `Missing`     | `on_disconnected` resets them before clearing `in_flight`                                    |
| `needed` bitfield and `PieceManager` bitfield stay in sync         | `on_write_completed` calls both `needed.unset_bit` and `pieces.storage_completed`            |
| A piece is re-queued for retry after hash failure                  | `on_message` calls `needed.set_bit` when `PieceInvalid` is returned                          |
