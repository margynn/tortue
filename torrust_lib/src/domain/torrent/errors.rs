use super::super::bencode::Error as BencodeError;
use super::PIECE_HASH_LEN;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("bencode parsing failed: {0}")]
    Bencode(#[from] BencodeError),

    #[error("invalid UTF-8 string")]
    InvalidUtf8,

    #[error("unexpected bencode type")]
    UnexpectedType,

    #[error("length value is negative")]
    NegativeLength,

    #[error("pieces data length is not a multiple of {PIECE_HASH_LEN}")]
    InvalidPiecesLength,
}

pub type Result<T> = std::result::Result<T, Error>;
