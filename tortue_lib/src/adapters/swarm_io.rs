use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::{
    sync::{mpsc, watch},
    time,
};
use tracing::info;

use crate::{
    application::ports::{peer_connector::PeerConnector, piece_store::PieceStore},
    domain::{
        message::Message,
        peer::PeerEvent,
        swarm::{Input, Output, Swarm, SwarmCommand, SwarmSnapshot, Tick},
        torrent::Metainfo,
        tracker::SessionStats,
    },
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("tracker disconnected")]
    TrackerDisconnected,
}

type Result<T> = std::result::Result<T, Error>;

pub struct SwarmIO<S, C> {
    metainfo: Arc<Metainfo>,
    commands_rx: mpsc::Receiver<SwarmCommand>,
    peers_rx: mpsc::Receiver<Vec<SocketAddr>>,
    peer_cmds: HashMap<SocketAddr, mpsc::Sender<Message>>,
    peer_events_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    peer_events_rx: mpsc::Receiver<(SocketAddr, PeerEvent)>,
    piece_store: S,
    peer_connector: C,
    progress_tx: watch::Sender<SwarmSnapshot>,
    stats: Arc<Mutex<SessionStats>>,
    rate_sample: RateSample,
}

#[derive(Debug, Default)]
struct RateSample {
    at: Option<Instant>,
    global_uploaded: u64,
    global_downloaded: u64,
    upload_rate: f64,
    download_rate: f64,
    per_peer: HashMap<SocketAddr, PeerSample>,
}

#[derive(Debug, Clone, Copy, Default)]
struct PeerSample {
    uploaded: u64,
    downloaded: u64,
    upload_rate: f64,
    download_rate: f64,
}

impl<S: PieceStore, C: PeerConnector> SwarmIO<S, C> {
    const BLOCK_TICK_INTERVAL: Duration = Duration::from_secs(10);
    const PEX_TICK_INTERVAL: Duration = Duration::from_secs(60);
    const RATE_TICK_INTERVAL: Duration = Duration::from_secs(2);

    pub fn new(
        metainfo: Arc<Metainfo>,
        peers_rx: mpsc::Receiver<Vec<SocketAddr>>,
        peer_connector: C,
        piece_store: S,
        progress_tx: watch::Sender<SwarmSnapshot>,
        stats: Arc<Mutex<SessionStats>>,
        commands_rx: mpsc::Receiver<SwarmCommand>,
    ) -> Self {
        let (peer_events_tx, peer_events_rx) = mpsc::channel(1024);
        Self {
            metainfo,
            commands_rx,
            peers_rx,
            peer_cmds: HashMap::new(),
            peer_events_tx,
            peer_events_rx,
            piece_store,
            peer_connector,
            progress_tx,
            stats,
            rate_sample: RateSample::default(),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut coordinator = Swarm::new(Arc::clone(&self.metainfo));
        let mut block_tick = time::interval(Self::BLOCK_TICK_INTERVAL);
        let mut pex_tick = time::interval(Self::PEX_TICK_INTERVAL);

        loop {
            let input = tokio::select! {
                // Listen to application commands
                command = self.commands_rx.recv() => match command {
                    Some(cmd) => Input::SwarmCommand(cmd),
                    None => break,
                },

                // Listen to trackers
                addrs = self.peers_rx.recv() => match addrs {
                    Some(addrs) => Input::PeersDiscovered(addrs),
                    None => return Err(Error::TrackerDisconnected),
                },

                // Listen to peers
                msg = self.peer_events_rx.recv() => match msg {
                    None => break,
                    Some((addr, PeerEvent::Connected{peer_id, peer_extensions})) => {
                        info!(addr = %addr, peer_id = %peer_id, "peer connected");
                        Input::PeerConnected { addr, peer_extensions }
                    },
                    Some((addr, PeerEvent::Disconnected)) => {
                        info!(addr = %addr, "peer disconnected");
                        self.peer_cmds.remove(&addr);
                        Input::PeerDisconnected(addr)
                    },
                    Some((addr, PeerEvent::MessageReceived(message))) => {
                        Input::MessageReceived { addr, message }
                    },
                },

                // Tick block
                _ = block_tick.tick() => Input::Tick(Tick::Block),

                // Tick PEX
                _ = pex_tick.tick() => Input::Tick(Tick::Pex),

            };

            for out in coordinator.step(input) {
                self.handle_output(out);
            }

            let snapshot = coordinator.snapshot();
            self.publish(snapshot);
        }

        Ok(())
    }

    fn handle_output(&mut self, out: Output) {
        match out {
            Output::ConnectPeer(addr) => {
                self.spawn_peer(addr);
            },
            Output::DisconnectPeer(addr) => {
                self.peer_cmds.remove(&addr);
                self.peer_connector.disconnect(addr);
            },
            Output::SendToPeer { addr, message } => {
                if let Some(tx) = self.peer_cmds.get(&addr) {
                    let _ = tx.try_send(message);
                }
            },
            Output::Completed => {
                info!("download completed");
                // todo: hook
            },
            Output::WritePiece { offset, data } => {
                if let Err(e) = self.piece_store.write(offset, &data) {
                    tracing::error!(error = %e, "failed to write piece");
                }
            },
            Output::Broadcast(message) => {
                for tx in self.peer_cmds.values() {
                    let _ = tx.try_send(message.clone());
                }
            },
        }
    }

    fn spawn_peer(&mut self, addr: SocketAddr) {
        if self.peer_cmds.contains_key(&addr) {
            return;
        }
        let (cmd_tx, cmd_rx) = mpsc::channel(256);
        self.peer_cmds.insert(addr, cmd_tx);
        self.peer_connector
            .connect(addr, cmd_rx, self.peer_events_tx.clone());
    }

    fn publish(&mut self, mut snapshot: SwarmSnapshot) {
        *self.stats.lock().unwrap() = SessionStats {
            swarm_status: snapshot.status,
            uploaded: snapshot.bytes_uploaded,
            downloaded: snapshot.bytes_downloaded,
            left: snapshot
                .bytes_total
                .saturating_sub(snapshot.bytes_downloaded),
        };
        self.apply_rates(&mut snapshot);
        let _ = self.progress_tx.send(snapshot);
    }

    fn apply_rates(&mut self, snapshot: &mut SwarmSnapshot) {
        self.rate_sample
            .update(snapshot, Instant::now(), Self::RATE_TICK_INTERVAL);

        snapshot.download_rate = self.rate_sample.download_rate;
        snapshot.upload_rate = self.rate_sample.upload_rate;

        for peer in &mut snapshot.peers {
            if let Some(sample) = self.rate_sample.per_peer.get(&peer.addr) {
                peer.upload_rate = sample.upload_rate;
                peer.download_rate = sample.download_rate;
            } else {
                peer.upload_rate = 0.0;
                peer.download_rate = 0.0;
            }
        }
    }
}

impl RateSample {
    fn update(&mut self, snapshot: &SwarmSnapshot, now: Instant, interval: Duration) {
        let Some(previous_at) = self.at else {
            self.record(snapshot, now);
            return;
        };

        let elapsed = now.duration_since(previous_at);

        if elapsed < interval {
            return;
        }

        let elapsed = elapsed.as_secs_f64();

        self.download_rate = snapshot
            .bytes_downloaded
            .saturating_sub(self.global_downloaded) as f64
            / elapsed;

        self.upload_rate =
            snapshot.bytes_uploaded.saturating_sub(self.global_uploaded) as f64 / elapsed;

        for peer in &snapshot.peers {
            let previous = self.per_peer.get(&peer.addr).copied().unwrap_or_default();

            let upload_rate =
                peer.bytes_uploaded.saturating_sub(previous.uploaded) as f64 / elapsed;

            let download_rate =
                peer.bytes_downloaded.saturating_sub(previous.downloaded) as f64 / elapsed;

            self.per_peer.insert(
                peer.addr,
                PeerSample {
                    uploaded: peer.bytes_uploaded,
                    downloaded: peer.bytes_downloaded,
                    upload_rate,
                    download_rate,
                },
            );
        }

        self.record_global(snapshot, now);
    }

    fn record(&mut self, snapshot: &SwarmSnapshot, now: Instant) {
        self.record_global(snapshot, now);

        self.per_peer = snapshot
            .peers
            .iter()
            .map(|peer| {
                (
                    peer.addr,
                    PeerSample {
                        uploaded: peer.bytes_uploaded,
                        downloaded: peer.bytes_downloaded,
                        ..Default::default()
                    },
                )
            })
            .collect();
    }

    fn record_global(&mut self, snapshot: &SwarmSnapshot, now: Instant) {
        self.at = Some(now);
        self.global_uploaded = snapshot.bytes_uploaded;
        self.global_downloaded = snapshot.bytes_downloaded;
    }
}
