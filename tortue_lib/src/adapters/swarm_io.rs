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
        swarm::{Input, Output, Swarm, SwarmSnapshot},
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
    peers_rx: mpsc::Receiver<Vec<SocketAddr>>,
    peer_cmds: HashMap<SocketAddr, mpsc::Sender<Message>>,
    peer_events_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    peer_events_rx: mpsc::Receiver<(SocketAddr, PeerEvent)>,
    piece_store: S,
    peer_connector: C,
    progress_tx: watch::Sender<SwarmSnapshot>,
    stats: Arc<Mutex<SessionStats>>,
    last_sample: RateSample,
}

#[derive(Default)]
struct RateSample {
    at: Option<Instant>,
    global_uploaded: u64,
    global_downloaded: u64,
    download_rate: f64,
    upload_rate: f64,
}

impl<S: PieceStore, C: PeerConnector> SwarmIO<S, C> {
    const TICK_INTERVAL: Duration = Duration::from_secs(20);
    const RATE_INTERVAL: Duration = Duration::from_secs(1);

    pub fn new(
        metainfo: Arc<Metainfo>,
        peers_rx: mpsc::Receiver<Vec<SocketAddr>>,
        peer_connector: C,
        piece_store: S,
        progress_tx: watch::Sender<SwarmSnapshot>,
        stats: Arc<Mutex<SessionStats>>,
    ) -> Self {
        let (peer_events_tx, peer_events_rx) = mpsc::channel(1024);
        Self {
            metainfo,
            peers_rx,
            peer_cmds: HashMap::new(),
            peer_events_tx,
            peer_events_rx,
            piece_store,
            peer_connector,
            progress_tx,
            stats,
            last_sample: RateSample::default(),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut coordinator = Swarm::new(Arc::clone(&self.metainfo));
        let mut tick = time::interval(Self::TICK_INTERVAL);

        loop {
            let input = tokio::select! {
                addrs = self.peers_rx.recv() => match addrs {
                    Some(addrs) => Input::PeersDiscovered(addrs),
                    None => return Err(Error::TrackerDisconnected),
                },

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

                _ = tick.tick() => Input::Tick,

                // _ = rate_tick.tick() => {
                //     let snapshot = coordinator.snapshot();
                //     self.publish(snapshot);
                // },
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
            Output::ConnectPeer(addr) => self.spawn_peer(addr),
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
        let now = Instant::now();
        let should_sample = match self.last_sample.at {
            None => true,
            Some(prev_at) => now.duration_since(prev_at) >= Self::RATE_INTERVAL,
        };

        if should_sample {
            if let Some(prev_at) = self.last_sample.at {
                let elapsed = now.duration_since(prev_at).as_secs_f64();
                self.last_sample.download_rate = (snapshot.bytes_downloaded as u64)
                    .saturating_sub(self.last_sample.global_downloaded)
                    as f64
                    / elapsed;
                self.last_sample.upload_rate = (snapshot.bytes_uploaded as u64)
                    .saturating_sub(self.last_sample.global_uploaded)
                    as f64
                    / elapsed;
            }
            self.last_sample.at = Some(now);
            self.last_sample.global_downloaded = snapshot.bytes_downloaded as u64;
            self.last_sample.global_uploaded = snapshot.bytes_uploaded as u64;
        }

        snapshot.download_rate = self.last_sample.download_rate;
        snapshot.upload_rate = self.last_sample.upload_rate;
    }
}
