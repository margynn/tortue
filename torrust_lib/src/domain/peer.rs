mod address;
mod message;
mod state;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid message")]
    InvalidMessage,
}
