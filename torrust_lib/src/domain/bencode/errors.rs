#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("unexpected end of input")]
    UnexpectedEof,

    #[error("expected byte {expected:#x}, found {found:#x}")]
    UnexpectedByte { expected: u8, found: u8 },

    #[error("invalid token: {0:#x}")]
    InvalidToken(u8),

    #[error("invalid integer")]
    InvalidInteger,

    #[error("invalid string length")]
    InvalidStringLength,

    #[error("invalid string")]
    InvalidString,

    #[error("invalid dictionary key")]
    InvalidDictKey,

    #[error("key not found")]
    KeyNotFound,

    #[error("type mismatch")]
    TypeMismatch,
}

pub type Result<T> = std::result::Result<T, Error>;
