pub mod bitfield;
mod client;
mod peer;
mod state;
mod swarm;

pub use peer::{PeerAddr, PeerId};
pub use swarm::Swarm;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("jion error: {0}")]
    JoinError(#[from] tokio::task::JoinError),

    #[error("piece out of range")]
    PieceOutOfRange,

    #[error("timeout")]
    Timeout,

    #[error("invalid handshake: {0}")]
    InvalidHandshake(&'static str),

    #[error("info hash mismatch")]
    InfoHashMismatch,

    #[error("peer id mismatch")]
    PeerIdMismatch,

    #[error("invalid message")]
    InvalidMessage,
}

pub enum PeerMessage {
    // move to wire
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
