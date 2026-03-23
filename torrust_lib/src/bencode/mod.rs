mod decoder;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bencode<'a> {
    Int(i64),
    Bytes(&'a [u8]),
    List(Vec<Bencode<'a>>),
    Dict(Vec<(&'a [u8], Bencode<'a>)>),
}

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
    #[error("invalid dictionary key")]
    InvalidDictKey,
}

impl Bencode<'_> {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        todo!("encoding not yet implemented")
    }
}

/// Decodes bencode data into a `Bencode` value.
///
/// # Errors
///
/// Returns an error if the input is not valid bencode format.
pub fn decode(data: &[u8]) -> Result<Bencode<'_>, Error> {
    decoder::Decoder::new(data).parse()
}
