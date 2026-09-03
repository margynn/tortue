use std::collections::HashMap;
use std::net::SocketAddr;

use tokio::sync::mpsc;
use tracing::info;

use crate::adapters::peer_runner::{PeerEvent, PeerRunner};
use crate::domain::peer::{self, PeerId};
use crate::domain::pool::{self, Pool};
use crate::domain::torrent::Metainfo;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("tracker disconnected")]
    TrackerDisconnected,
}

type Result<T> = std::result::Result<T, Error>;

pub struct PoolRunner {
    metainfo: Metainfo,
    client_id: PeerId,
    peers_rx: mpsc::Receiver<Vec<SocketAddr>>,
    peer_cmds: HashMap<SocketAddr, mpsc::Sender<peer::Message>>,
    pool_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    pool_rx: mpsc::Receiver<(SocketAddr, PeerEvent)>,
    verified_pieces: usize,
}

impl PoolRunner {
    pub fn new(
        metainfo: Metainfo,
        client_id: PeerId,
        peers_rx: mpsc::Receiver<Vec<SocketAddr>>,
    ) -> Self {
        let (pool_tx, pool_rx) = mpsc::channel(256);
        Self {
            client_id,
            metainfo,
            peers_rx,
            peer_cmds: HashMap::new(),
            pool_tx,
            pool_rx,
            verified_pieces: 0,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut pool = Pool::new(self.metainfo.clone());

        loop {
            let input = tokio::select! {
                addrs = self.peers_rx.recv() => match addrs {
                    Some(addrs) => pool::Input::PeersDiscovered(addrs),
                    None => return Err(Error::TrackerDisconnected),
                },

                msg = self.pool_rx.recv() => match msg {
                    None => break,
                    Some((addr, PeerEvent::Connected(peer_id))) => pool::Input::PeerConnected { addr, peer_id },
                    Some((addr, PeerEvent::Disconnected)) => pool::Input::PeerDisconnected(addr),
                    Some((addr, PeerEvent::MessageReceived(message))) => pool::Input::MessageReceived { addr, message },
                },
            };

            match &input {
                pool::Input::PeerDisconnected(addr) => {
                    self.peer_cmds.remove(addr);
                },
                pool::Input::PieceVerified(_) => {
                    self.verified_pieces += 1;
                    let total = self.metainfo.pieces.len();
                    info!(piece = self.verified_pieces, total, "piece verified");
                },
                _ => {},
            }

            for out in pool.step(input) {
                self.handle_output(out).await;
            }
        }

        Ok(())
    }

    async fn handle_output(&mut self, out: pool::Output) {
        match out {
            pool::Output::ConnectPeer(addr) => self.spawn_peer(addr),
            pool::Output::SendToPeer { addr, message } => {
                if let Some(v) = self.peer_cmds.get(&addr) {
                    let _ = v.send(message).await;
                }
            },
            pool::Output::Completed => info!("download completed"),
        }
    }

    fn spawn_peer(&mut self, addr: SocketAddr) {
        // cmd: pool_runner → peer_runner (messages to write to TCP)
        let (cmd_tx, cmd_rx) = mpsc::channel(128);
        // peer_out: peer_runner → forwarding task (lifecycle events + messages received)
        let (peer_out_tx, mut peer_out_rx) = mpsc::channel(128);

        self.peer_cmds.insert(addr, cmd_tx);

        // Task 1: pure IO — TCP connect/read/write, no domain logic.
        let metainfo = self.metainfo.clone();
        let mut runner =
            PeerRunner::new(addr, self.client_id, metainfo.into(), cmd_rx, peer_out_tx);
        tokio::spawn(async move { runner.run().await });

        // Task 2: forward PeerEvents from the peer runner to the pool, with logging.
        // Sends a final Disconnected sentinel when the peer runner stops, so Pool
        // always cleans up even if the runner exited without sending one.
        let pool_tx = self.pool_tx.clone();
        tokio::spawn(async move {
            while let Some(event) = peer_out_rx.recv().await {
                match &event {
                    PeerEvent::Connected(peer_id) => {
                        info!(addr = %addr, peer_id = %peer_id, "peer connected");
                    },
                    PeerEvent::Disconnected => {
                        info!(addr = %addr, "peer disconnected");
                    },
                    PeerEvent::MessageReceived(msg) => match msg {
                        peer::Message::Unchoke
                        | peer::Message::Choke
                        | peer::Message::Bitfield(_)
                        | peer::Message::Have(_) => {
                            tracing::debug!(addr = %addr, msg = ?msg, "peer state");
                        },
                        _ => {
                            tracing::trace!(addr = %addr, msg = ?msg, "peer data");
                        },
                    },
                }
                if pool_tx.send((addr, event)).await.is_err() {
                    break;
                }
            }

            // Sentinel: guarantees Pool receives Disconnected even on abnormal exit.
            // Pool handles duplicate Disconnected gracefully (peers.remove is idempotent).
            let _ = pool_tx.send((addr, PeerEvent::Disconnected)).await;
        });
    }
}
