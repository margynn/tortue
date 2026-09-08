use std::collections::HashMap;
use std::net::SocketAddr;

use sha1::{Digest, Sha1};

use crate::domain::peer::{PeerExtensions, PeerId};

use super::message::{Message, UtMetadataMessage};
use super::torrent::InfoHash;

pub enum Input {
    PeersDiscovered(Vec<SocketAddr>),
    PeerConnected {
        addr: SocketAddr,
        peer_id: PeerId,
        peer_extensions: PeerExtensions,
    },
    PeerDisconnected(SocketAddr),
    MessageReceived {
        addr: SocketAddr,
        message: Message,
    },
}

pub enum Output {
    ConnectPeer(SocketAddr),
    SendToPeer { addr: SocketAddr, message: Message },
    Done(Vec<u8>), // raw validated info bytes
}

pub struct Metadata {
    info_hash: InfoHash,
    total_size: Option<usize>,
    pieces: Vec<Option<Vec<u8>>>,
    peers: HashMap<SocketAddr, u8>, // peer -> ut_metadata ext_id
}

impl Metadata {
    const PIECE_SIZE: usize = 16 * 1024; // 16Kib

    pub fn new(info_hash: InfoHash) -> Self {
        Self {
            info_hash,
            total_size: None,
            pieces: vec![],
            peers: HashMap::new(),
        }
    }

    pub fn step(&mut self, input: Input) -> Vec<Output> {
        match input {
            Input::PeersDiscovered(addrs) => addrs
                .iter()
                .map(|addr| Output::ConnectPeer(*addr))
                .collect(),
            Input::PeerDisconnected(addr) => {
                self.peers.remove(&addr);
                vec![]
            },
            Input::MessageReceived { addr, message } => self.on_message(addr, message),
            Input::PeerConnected { .. } => vec![],
        }
    }

    fn on_message(&mut self, addr: SocketAddr, message: Message) -> Vec<Output> {
        match message {
            Message::ExtensionHandshake(hs) => {
                let Some(ext_id) = hs.extensions.get("ut_metadata") else {
                    return vec![];
                };
                let Some(metadata_size) = hs.metadata_size else {
                    return vec![];
                };
                if self.total_size.is_none() {
                    let count = metadata_size.div_ceil(Self::PIECE_SIZE);
                    self.total_size = Some(metadata_size);
                    self.pieces = vec![None; count];
                }
                self.peers.insert(addr, *ext_id);
                self.request_missing_from(addr)
            },
            Message::Extension { ext_id, payload } => {
                if self.peers.get(&addr) != Some(&ext_id) {
                    return vec![];
                }
                let Ok(msg) = UtMetadataMessage::decode(&payload) else {
                    return vec![];
                };
                match msg {
                    UtMetadataMessage::Data { piece, data, .. } => {
                        if piece >= self.pieces.len() || self.pieces[piece].is_some() {
                            return vec![];
                        }
                        self.pieces[piece] = Some(data);
                        if self.pieces.iter().all(|p| p.is_some()) {
                            self.try_finalize()
                        } else {
                            vec![]
                        }
                    },
                    UtMetadataMessage::Reject { piece } => {
                        let other = self.peers.keys().find(|&&a| a != addr).copied();
                        match other {
                            Some(a) => self.request_piece_from(a, piece),
                            None => vec![],
                        }
                    },
                    _ => vec![],
                }
            },
            _ => vec![],
        }
    }

    fn request_missing_from(&self, addr: SocketAddr) -> Vec<Output> {
        let Some(&ext_id) = self.peers.get(&addr) else {
            return vec![];
        };
        self.pieces
            .iter()
            .enumerate()
            .filter(|(_, p)| p.is_none())
            .map(|(piece, _)| self.make_request(addr, ext_id, piece))
            .collect()
    }

    fn request_piece_from(&self, addr: SocketAddr, piece: usize) -> Vec<Output> {
        let Some(&ext_id) = self.peers.get(&addr) else {
            return vec![];
        };
        vec![self.make_request(addr, ext_id, piece)]
    }

    fn make_request(&self, addr: SocketAddr, ext_id: u8, piece: usize) -> Output {
        Output::SendToPeer {
            addr,
            message: Message::Extension {
                ext_id,
                payload: UtMetadataMessage::Request { piece }.encode(),
            },
        }
    }

    fn try_finalize(&mut self) -> Vec<Output> {
        let bytes: Vec<u8> = self
            .pieces
            .iter()
            .flat_map(|p| p.as_deref().unwrap_or_default())
            .copied()
            .collect();

        let hash: [u8; 20] = Sha1::digest(&bytes).into();
        if hash == self.info_hash.as_ref() {
            return vec![Output::Done(bytes)];
        }

        // SHA1 mismatch: discard and re-request from all known peers
        self.pieces.iter_mut().for_each(|p| *p = None);
        let peers: Vec<SocketAddr> = self.peers.keys().copied().collect();
        peers
            .into_iter()
            .flat_map(|a| self.request_missing_from(a))
            .collect()
    }
}
