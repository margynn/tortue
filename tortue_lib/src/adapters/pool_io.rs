use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use tokio::sync::{mpsc, watch};
use tracing::info;

use crate::{
    application::ports::{
        peer_connector::{PeerConnector, PeerEvent},
        piece_store::PieceStore,
    },
    domain::{
        message::Message,
        pool::{Input, Output, Pool, PoolSnapshot},
        torrent::Metainfo,
    },
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("tracker disconnected")]
    TrackerDisconnected,
}

type Result<T> = std::result::Result<T, Error>;

pub struct PoolIO<S, C> {
    metainfo: Arc<Metainfo>,
    peers_rx: mpsc::Receiver<Vec<SocketAddr>>,
    peer_cmds: HashMap<SocketAddr, mpsc::Sender<Message>>,
    pool_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    pool_rx: mpsc::Receiver<(SocketAddr, PeerEvent)>,
    piece_store: S,
    peer_connector: C,
    progress_tx: watch::Sender<PoolSnapshot>,
}

impl<S: PieceStore, C: PeerConnector> PoolIO<S, C> {
    pub fn new(
        metainfo: Arc<Metainfo>,
        peers_rx: mpsc::Receiver<Vec<SocketAddr>>,
        peer_connector: C,
        piece_store: S,
        progress_tx: watch::Sender<PoolSnapshot>,
    ) -> Self {
        let (pool_tx, pool_rx) = mpsc::channel(256);
        Self {
            metainfo,
            peers_rx,
            peer_cmds: HashMap::new(),
            pool_tx,
            pool_rx,
            piece_store,
            peer_connector,
            progress_tx,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut pool = Pool::new(Arc::clone(&self.metainfo));

        loop {
            let input = tokio::select! {
                addrs = self.peers_rx.recv() => match addrs {
                    Some(addrs) => Input::PeersDiscovered(addrs),
                    None => return Err(Error::TrackerDisconnected),
                },

                msg = self.pool_rx.recv() => match msg {
                    None => break,
                    Some((addr, PeerEvent::Connected(peer_id))) => {
                        info!(addr = %addr, peer_id = %peer_id, "peer connected");
                        Input::PeerConnected { addr, peer_id }
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
            };

            for out in pool.step(input) {
                self.handle_output(out).await;
            }

            let _ = self.progress_tx.send(pool.snapshot());
        }

        Ok(())
    }

    async fn handle_output(&mut self, out: Output) {
        match out {
            Output::ConnectPeer(addr) => self.spawn_peer(addr),
            Output::SendToPeer { addr, message } => {
                if let Some(tx) = self.peer_cmds.get(&addr) {
                    let _ = tx.send(message).await;
                }
            },
            Output::Completed => info!("download completed"),
            Output::WritePiece { offset, data } => {
                if let Err(e) = self.piece_store.write(offset, &data).await {
                    tracing::error!(error = %e, "failed to write piece");
                }
            },
            Output::Broadcast(message) => {
                for tx in self.peer_cmds.values() {
                    let _ = tx.send(message.clone()).await;
                }
            },
        }
    }

    fn spawn_peer(&mut self, addr: SocketAddr) {
        let (cmd_tx, cmd_rx) = mpsc::channel(256);
        self.peer_cmds.insert(addr, cmd_tx);
        self.peer_connector
            .connect(addr, cmd_rx, self.pool_tx.clone());
    }
}
