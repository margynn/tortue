mod address;
mod handshake;
mod message;
mod session;
mod state;

pub use address::{PeerAddr, PeerId};
pub use message::Message;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid message")]
    InvalidMessage,

    #[error("invalid handshake: {0}")]
    InvalidHandshake(&'static str),
}
