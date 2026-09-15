use std::{collections::HashMap, net::SocketAddr};

use tokio::sync::mpsc;

use crate::{
    application::ports::peer_connector::PeerConnector,
    domain::{
        magnet::MagnetLink,
        message::Message,
        metadata::{Input, Metadata, Output},
        peer::PeerEvent,
    },
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("tracker disconnected")]
    TrackerDisconnected,
}
type Result<T> = std::result::Result<T, Error>;

pub struct MetadataIO<C> {
    magnet: MagnetLink,
    peers_rx: mpsc::Receiver<Vec<SocketAddr>>,
    peer_cmds: HashMap<SocketAddr, mpsc::Sender<Message>>,
    peer_events_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    peer_events_rx: mpsc::Receiver<(SocketAddr, PeerEvent)>,
    peer_connector: C,
}

impl<C: PeerConnector> MetadataIO<C> {
    pub fn new(
        magnet: MagnetLink,
        peers_rx: mpsc::Receiver<Vec<SocketAddr>>,
        peer_connector: C,
    ) -> Self {
        let (peer_events_tx, peer_events_rx) = mpsc::channel(1024);
        Self {
            magnet,
            peers_rx,
            peer_cmds: HashMap::new(),
            peer_events_tx,
            peer_events_rx,
            peer_connector,
        }
    }

    pub async fn run(&mut self) -> Result<Vec<u8>> {
        let mut metadata_fetcher = Metadata::new(self.magnet.info_hash);

        let initial_peers: Vec<SocketAddr> = self
            .magnet
            .peers
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        if !initial_peers.is_empty() {
            for out in metadata_fetcher.step(Input::PeersDiscovered(initial_peers)) {
                if let Some(buffer) = self.handle_output(out) {
                    return Ok(buffer);
                }
            }
        }

        loop {
            let input = tokio::select! {
                addrs = self.peers_rx.recv() => match addrs {
                    Some(addrs) => Input::PeersDiscovered(addrs),
                    None => return Err(Error::TrackerDisconnected),
                },

                msg = self.peer_events_rx.recv() => match msg {
                    None => break,
                    Some((_, PeerEvent::Connected { .. })) => continue,
                    Some((addr, PeerEvent::Disconnected)) => {
                        self.peer_cmds.remove(&addr);
                        Input::PeerDisconnected(addr)
                    },
                    Some((addr, PeerEvent::MessageReceived(message))) => {
                        Input::MessageReceived { addr, message }
                    },
                },
            };

            for out in metadata_fetcher.step(input) {
                if let Some(buffer) = self.handle_output(out) {
                    return Ok(buffer);
                }
            }
        }

        Ok(vec![])
    }

    fn handle_output(&mut self, out: Output) -> Option<Vec<u8>> {
        match out {
            Output::ConnectPeer(addr) => self.spawn_peer(addr),
            Output::SendToPeer { addr, message } => {
                if let Some(tx) = self.peer_cmds.get(&addr) {
                    let _ = tx.try_send(message);
                }
            },
            Output::Done(items) => return Some(items),
        };
        None
    }

    fn spawn_peer(&mut self, addr: SocketAddr) {
        let (cmd_tx, cmd_rx) = mpsc::channel(256);
        self.peer_cmds.insert(addr, cmd_tx);
        self.peer_connector
            .connect(addr, cmd_rx, self.peer_events_tx.clone());
    }
}
