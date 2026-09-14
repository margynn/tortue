# Per-peer + global upload/download rate tracking

## Context

`Swarm` already tracks cumulative upload/download bytes (globally, via `PieceManager.uploaded_bytes`/`downloaded_bytes`) and exposes them on `SwarmSnapshot`. There's no rate (bytes/sec) and no per-peer breakdown. The user wants both: per-peer AND global rates, and wants the CLI progress bar updated to show it too.

Domain layer (`tortue_lib/src/domain/`) is deliberately I/O- and time-free — the only exception is `Instant`-based timeout tracking inside `BlockAssignments`. Rate math needs wall-clock deltas, so it belongs in the adapter layer (`SwarmIO`), not in `Swarm`. The domain layer's job is just to expose cumulative byte counters (global, already done; per-peer, new) — the adapter samples those over time to derive rates.

## 1. `tortue_lib/src/domain/swarm/peer_registry.rs`

... DONE

## 3. `tortue_lib/src/adapters/swarm_io.rs`

Keep it simple — no new file, inline rate computation here. Add a private struct and a field:

```rust
#[derive(Default)]
struct RateSample {
    at: Option<Instant>,
    global_uploaded: u64,
    global_downloaded: u64,
    per_peer: HashMap<SocketAddr, (u64, u64)>, // addr -> (uploaded, downloaded)
}
```

`SwarmIO` gains `last_sample: RateSample` (init via `RateSample::default()` in `new`), and a new constant `const RATE_INTERVAL: Duration = Duration::from_secs(1);` beside `TICK_INTERVAL`. Add `use std::time::Instant;`.

Restructure `run()`'s loop: the rate tick doesn't drive `Swarm::step` (it only annotates the outgoing snapshot), so it must NOT become a domain `Input` variant. Factor the existing "step + snapshot + stats + send" body into an `apply()` helper, and add a `publish_rates()` path for the new tick, both funneling into a shared `publish()`:

```rust
pub async fn run(&mut self) -> Result<()> {
    let mut coordinator = Swarm::new(Arc::clone(&self.metainfo));
    let mut tick = time::interval(Self::TICK_INTERVAL);
    let mut rate_tick = time::interval(Self::RATE_INTERVAL);

    loop {
        tokio::select! {
            addrs = self.peers_rx.recv() => match addrs {
                Some(addrs) => self.apply(&mut coordinator, Input::PeersDiscovered(addrs)),
                None => return Err(Error::TrackerDisconnected),
            },
            _ = tick.tick() => self.apply(&mut coordinator, Input::Tick),
            msg = self.peer_events_rx.recv() => match msg {
                None => break,
                Some((addr, PeerEvent::Connected{peer_id, peer_extensions})) => {
                    info!(addr = %addr, peer_id = %peer_id, "peer connected");
                    self.apply(&mut coordinator, Input::PeerConnected { addr, peer_extensions });
                },
                Some((addr, PeerEvent::Disconnected)) => {
                    info!(addr = %addr, "peer disconnected");
                    self.peer_cmds.remove(&addr);
                    self.apply(&mut coordinator, Input::PeerDisconnected(addr));
                },
                Some((addr, PeerEvent::MessageReceived(message))) => {
                    self.apply(&mut coordinator, Input::MessageReceived { addr, message });
                },
            },
            _ = rate_tick.tick() => {
                let snapshot = coordinator.snapshot();
                self.publish(snapshot);
            },
        }
    }

    Ok(())
}

fn apply(&mut self, coordinator: &mut Swarm, input: Input) {
    for out in coordinator.step(input) {
        self.handle_output(out);
    }
    let snapshot = coordinator.snapshot();
    self.publish(snapshot);
}

fn publish(&mut self, mut snapshot: SwarmSnapshot) {
    *self.stats.lock().unwrap() = SessionStats {
        uploaded: snapshot.bytes_uploaded,
        downloaded: snapshot.bytes_downloaded,
        left: snapshot.bytes_total.saturating_sub(snapshot.bytes_downloaded),
    };
    self.apply_rates(&mut snapshot);
    let _ = self.progress_tx.send(snapshot);
}

fn apply_rates(&mut self, snapshot: &mut SwarmSnapshot) {
    let now = Instant::now();
    if let Some(prev_at) = self.last_sample.at {
        let elapsed = now.duration_since(prev_at).as_secs_f64();
        if elapsed > 0.0 {
            snapshot.download_rate = (snapshot.bytes_downloaded as u64)
                .saturating_sub(self.last_sample.global_downloaded) as f64 / elapsed;
            snapshot.upload_rate = (snapshot.bytes_uploaded as u64)
                .saturating_sub(self.last_sample.global_uploaded) as f64 / elapsed;

            for peer in &mut snapshot.peers {
                let (prev_up, prev_down) = self.last_sample.per_peer
                    .get(&peer.addr).copied().unwrap_or((peer.bytes_uploaded, peer.bytes_downloaded));
                peer.upload_rate = peer.bytes_uploaded.saturating_sub(prev_up) as f64 / elapsed;
                peer.download_rate = peer.bytes_downloaded.saturating_sub(prev_down) as f64 / elapsed;
            }
        }
    }

    self.last_sample = RateSample {
        at: Some(now),
        global_uploaded: snapshot.bytes_uploaded as u64,
        global_downloaded: snapshot.bytes_downloaded as u64,
        per_peer: snapshot.peers.iter()
            .map(|p| (p.addr, (p.bytes_uploaded, p.bytes_downloaded)))
            .collect(),
    };
}
```

`snapshot()` gets called a bit more often (every `Input` plus every 1s rate tick) — acceptable, it's a cheap O(peers+pieces) read.

**Pruning**: no explicit pruning needed. `last_sample.per_peer` is fully rebuilt from the live `snapshot.peers` list on every `apply_rates` call. `PeerRegistry::disconnected` already removes a peer from its `peers` map, so a disconnected peer is simply absent from the next `snapshot.peers` and drops out of `last_sample.per_peer` automatically. A reconnecting peer gets a fresh `PeerState` starting at 0, so no stale-history issue either.

## 4. `src/main.rs`

Extend the progress bar's `set_message` to show global rates via the existing `human_size()` helper:

```rust
bar.set_message(format!(
    "{} seeders, {} leechers, {} in flight — ↓ {}/s ↑ {}/s",
    s.seeders.len(),
    s.leechers.len(),
    s.blocks_in_flight,
    human_size(s.download_rate as u64),
    human_size(s.upload_rate as u64),
));
```

Per-peer rates (`s.peers`) are exposed on `SwarmSnapshot` for future consumers (e.g. a future `ratatui` table — `ratatui`/`crossterm` are already unused deps in `Cargo.toml`), but aren't rendered in the single-line `indicatif` bar; a bar isn't a table. This satisfies "update the CLI too" at the level a progress bar can meaningfully show.

## Verification

1. `cargo check` (workspace) — confirm new fields/methods compile, and `SwarmSnapshot::default()` (used in `application/download.rs`) still works (`Vec`/`f64` all implement `Default`).
2. `cargo clippy --all-targets` — check the refactored `run()` loop for lint issues.
3. Manual run: `cargo run -- download <torrent with several peers> -vv`, watch the progress bar; rates should read `0 B/s` for the first second (`last_sample.at` starts `None`), then track observed transfer speed.
4. Disconnect a peer mid-download — global rate shouldn't spike or go negative (`saturating_sub` guards against this defensively).
5. No existing unit tests touch `swarm.rs`/`peer_registry.rs`/adapters (`grep -rn "mod tests"` returned empty) — this plan doesn't add a test harness, consistent with current coverage.

## Critical files

- `tortue_lib/src/domain/swarm/peer_registry.rs`
- `tortue_lib/src/domain/swarm.rs`
- `tortue_lib/src/adapters/swarm_io.rs`
- `src/main.rs`
- `tortue_lib/src/domain/swarm/piece_manager.rs` (reference only — confirms existing duplicate-block accounting precedent, no changes needed)
