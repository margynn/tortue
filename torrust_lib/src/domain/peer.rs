mod handshake;
mod message;
mod session;

pub use handshake::{Handshake, PeerId};
pub use message::Message;
pub use session::{Input, Output, PeerSession};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid message")]
    InvalidMessage,

    #[error("invalid handshake: {0}")]
    InvalidHandshake(&'static str),
}
