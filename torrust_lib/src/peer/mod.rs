mod bitfield;
mod client;
mod handshake;

use std::net::IpAddr;

use rand::TryRng;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("piece out of range")]
    PieceOutOfRange,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("timeout")]
    Timeout,

    #[error("invalid handshake: {0}")]
    InvalidHandshake(&'static str),

    #[error("info hash mismatch")]
    InfoHashMismatch,

    #[error("peer id mismatch")]
    PeerIdMismatch,
}

#[derive(Debug)]
pub struct PeerClient {
    stream: tokio::net::TcpStream,
    peer: Peer,
    state: PeerState,
}

#[derive(Debug)]
pub struct PeerState {
    pub am_choking: bool,
    pub am_interested: bool,
    pub peer_choking: bool,
    pub peer_interested: bool,
    pub bitfield: bitfield::Bitfield,
}

pub enum PeerMessage {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Vec<u8>),
    Request { index: u32, begin: u32, length: u32 },
    Piece { index: u32, begin: u32, block: Vec<u8> },
    Cancel { index: u32, begin: u32, length: u32 },
}

#[derive(Debug, Clone)]
pub struct Peer {
    pub peer_id: Option<PeerId>,
    pub ip: IpAddr,
    pub port: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerId([u8; 20]);

impl PeerId {
    pub fn new(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    pub fn generate(client: &str, version: &str) -> Self {
        let mut id = [0u8; 20];

        let prefix = format!("-{}{}-", client, version);
        let prefix_bytes = prefix.as_bytes();

        let n = prefix_bytes.len().min(20);
        id[..n].copy_from_slice(&prefix_bytes[..n]);

        // new API in rand 0.9
        let mut rng = rand::rng();
        rng.try_fill_bytes(&mut id[n..]);

        Self(id)
    }
}

impl AsRef<[u8]> for PeerId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
