use std::collections::HashMap;
use std::net::SocketAddr;

use tokio::sync::mpsc;

use crate::adapters::peer_runner::PeerRunner;
use crate::domain::peer::{self, PeerId};
use crate::domain::pool::Pool;
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
    // pool_tx: mpsc::Sender<(SocketAddr, peer::Output)>,
    // pool_rx: mpsc::Receiver<(SocketAddr, peer::Output)>,
}

impl PoolRunner {
    pub fn new(
        metainfo: Metainfo,
        client_id: PeerId,
        peers_rx: mpsc::Receiver<Vec<SocketAddr>>,
    ) -> Self {
        Self {
            client_id,
            metainfo,
            peers_rx,
            peer_cmds: HashMap::new(),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut pool = Pool::new(self.metainfo.pieces.len());

        // TODO: use select!
        loop {
            if let Some(addrs) = self.peers_rx.recv().await {
                self.start_peers(addrs);
            }
        }

        Ok(())
    }

    fn spawn_peer(&mut self, addr: SocketAddr) {
        let (peer_out_tx, mut peer_out_rx) = mpsc::channel(128);
        let (cmd_tx, cmd_rx) = mpsc::channel(128);

        self.peer_cmds.insert(addr, cmd_tx);

        let metainfo = self.metainfo.clone();
        let mut runner =
            PeerRunner::new(addr, self.client_id, metainfo.into(), cmd_rx, peer_out_tx);
        tokio::spawn(async move { runner.run().await });

        // // Forward peer outputs tagged with addr into the shared pool channel
        // let pool_tx = self.pool_tx.clone();
        // tokio::spawn(async move {
        //     while let Some(out) = peer_out_rx.recv().await {
        //         if pool_tx.send((addr, out)).await.is_err() {
        //             break;
        //         }
        //     }
        // });
    }
}
