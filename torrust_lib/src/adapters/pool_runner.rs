use std::collections::HashMap;
use std::net::SocketAddr;

use tokio::sync::mpsc;
use tracing::info;

use crate::adapters::peer_runner::PeerRunner;
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
    peer_cmds: HashMap<SocketAddr, mpsc::Sender<peer::Input>>,
    pool_tx: mpsc::Sender<(SocketAddr, peer::Output)>,
    pool_rx: mpsc::Receiver<(SocketAddr, peer::Output)>,
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
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut pool = Pool::new(self.metainfo.pieces.len());

        loop {
            let input = tokio::select! {
                addrs = self.peers_rx.recv() => match addrs {
                    None => return Err(Error::TrackerDisconnected),
                    Some(addrs) => pool::Input::PeersDiscovered(addrs),
                },
                msg = self.pool_rx.recv() => match msg {
                    None => break,
                    Some((addr, out)) => pool::Input::FromPeer { addr, event: out },
                },
            };

            for out in pool.step(input) {
                self.handle_output(out);
            }
        }

        Ok(())
    }

    fn handle_output(&mut self, out: pool::Output) {
        match out {
            pool::Output::ConnectPeer(addr) => self.spawn_peer(addr),
            pool::Output::DisconnectPeer(addr) => {
                self.peer_cmds.remove(&addr);
            },
            pool::Output::RequestPiece { from, index } => {
                // TODO: PieceManager → block ranges → Message::Request
                let _ = self.peer_cmds.get(&from);
            },
            pool::Output::Completed => {
                // TODO: signal completion
            },
        }
    }

    fn spawn_peer(&mut self, addr: SocketAddr) {
        let (peer_out_tx, mut peer_out_rx) = mpsc::channel(128);
        let (cmd_tx, cmd_rx) = mpsc::channel(128);

        self.peer_cmds.insert(addr, cmd_tx);

        let metainfo = self.metainfo.clone();
        let mut runner = PeerRunner::new(addr, self.client_id, metainfo.into(), cmd_rx, peer_out_tx);
        tokio::spawn(async move { runner.run().await });

        let pool_tx = self.pool_tx.clone();
        tokio::spawn(async move {
            while let Some(out) = peer_out_rx.recv().await {
                match &out {
                    peer::Output::EmitConnected(peer_id) => {
                        info!(addr = %addr, peer_id = ?peer_id, "peer connected");
                    },
                    peer::Output::EmitDisconnected => {
                        info!(addr = %addr, "peer disconnected");
                    },
                    _ => {},
                }
                if pool_tx.send((addr, out)).await.is_err() {
                    break;
                }
            }
        });
    }
}
