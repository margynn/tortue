use std::{collections::HashMap, net::SocketAddr};

use tokio::sync::mpsc;

use crate::{
    InfoHash,
    application::ports::peer_connector::PeerConnector,
    domain::{magnet::MagnetLink, message::Message, peer::PeerEvent},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
type Result<T> = std::result::Result<T, Error>;

pub struct MetadataIO<C> {
    magnet: MagnetLink,
    peers_rx: mpsc::Receiver<Vec<SocketAddr>>,
    // peer_cmds: HashMap<SocketAddr, mpsc::Sender<Message>>,
    // peer_events_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    // peer_events_rx: mpsc::Receiver<(SocketAddr, PeerEvent)>,
    peer_connector: C,
}

impl<C: PeerConnector> MetadataIO<C> {
    pub fn new(
        magnet: MagnetLink,
        peers_rx: mpsc::Receiver<Vec<SocketAddr>>,
        peer_connector: C,
    ) -> Self {
        Self {
            magnet,
            peers_rx,
            // peer_cmds: (),
            // peer_events_tx: (),
            // peer_events_rx: (),
            peer_connector,
        }
    }

    pub async fn run(&mut self) -> Result<Vec<u8>> {
        todo!()
    }
}
