mod manager;
mod piece;
mod storage;

pub use manager::PieceManager;
pub use storage::{Storage, StorageCommand};
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
