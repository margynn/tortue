use std::{collections::HashMap, net::SocketAddr};

use tokio::sync::mpsc;

use crate::{
    InfoHash,
    domain::{message::Message, peer::PeerEvent},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
type Result<T> = std::result::Result<T, Error>;

pub struct MetadataIO {
    info_hash: InfoHash,
    trackers: Vec<String>,
    initial_peers: Vec<SocketAddr>, // from x.pe in magnet link
    peer_cmds: HashMap<SocketAddr, mpsc::Sender<Message>>,
    peer_events_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    peer_events_rx: mpsc::Receiver<(SocketAddr, PeerEvent)>,
}

impl MetadataIO {
    pub fn new() -> Self {
        Self {
            info_hash: (),
            trackers: (),
            initial_peers: (),
            peer_cmds: (),
            peer_events_tx: (),
            peer_events_rx: (),
        }
    }

    pub async fn run(
        info_hash: InfoHash,
        trackers: Vec<String>,
        initial_peers: Vec<SocketAddr>,
    ) -> Result<Vec<u8>> {
        todo!()
    }
}
