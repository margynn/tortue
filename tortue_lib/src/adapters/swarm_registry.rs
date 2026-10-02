use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use tokio::{net::TcpStream, sync::mpsc};

use crate::{InfoHash, domain::peer::Handshake};

#[derive(Clone, Default)]
pub struct SwarmRegistry {
    inner: Arc<Mutex<HashMap<InfoHash, mpsc::Sender<InboundPeer>>>>,
}

pub struct InboundPeer {
    pub addr: SocketAddr,
    pub handshake: Handshake,
    pub stream: TcpStream,
}

impl SwarmRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn register(&mut self, info_hash: InfoHash) {
        todo!()
    }
    // register(info_hash, tx) / unregister(info_hash) : appelés par start_download quand un SwarmIO démarre/s'arrête.
    // route(info_hash) -> Option<Sender<InboundPeer>> : utilisé par PeerListenner pour savoir si le metainfo est connu et à qui transmettre la connexion.
}
