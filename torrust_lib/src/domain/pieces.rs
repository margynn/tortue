mod manager;
mod piece;

pub use manager::*;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("piece error: {0}")]
    Piece(#[from] piece::PieceError),
}
