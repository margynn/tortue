use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex, RwLock},
};

use tokio::{net::TcpStream, sync::mpsc};

use crate::{InfoHash, domain::peer::Handshake};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("already registered")]
    AlreadyRegistered,
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Default)]
pub struct SwarmRegistry {
    inner: Arc<RwLock<HashMap<InfoHash, mpsc::Sender<(SocketAddr, InboundPeer)>>>>,
}

pub struct InboundPeer {
    pub addr: SocketAddr,
    pub handshake: Handshake,
    pub stream: TcpStream,
}

impl SwarmRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn register(&mut self, info_hash: InfoHash, tx: mpsc::Sender<(SocketAddr, InboundPeer)>) {
        let mut data = self.inner.write().unwrap();
        match data.entry(info_hash) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(tx);
            },
            std::collections::hash_map::Entry::Occupied(_) => {},
        };
    }

    pub fn unregister(&mut self, info_hash: InfoHash) {
        let mut data = self.inner.write().unwrap();
        data.remove(&info_hash);
    }

    pub fn route(&self, info_hash: InfoHash) -> Option<mpsc::Sender<(SocketAddr, InboundPeer)>> {
        let data = self.inner.read().unwrap();
        data.get(&info_hash).cloned()
    }
}
