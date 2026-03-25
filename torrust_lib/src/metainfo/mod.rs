mod decode;

pub use decode::decode;

const SHA_LENGTH: usize = 20;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("bencode parsing failed: {0}")]
    Bencode(#[from] crate::bencode::Error),
    #[error("invalid UTF-8 string")]
    InvalidUtf8String,
    #[error("invalid dictionary key")]
    InvalidDictKey,
}

#[derive(Debug, Clone)]
pub struct Metainfo {
    announce: String,
    name: String,
    hash: [u8; SHA_LENGTH],
    piece_length: usize,
    pieces: Vec<[u8; SHA_LENGTH]>,
    mode: Mode,
}

#[derive(Debug, Clone)]
pub enum Mode {
    Single { length: usize },
    Multiple { files: Vec<File> },
}

#[derive(Debug, Clone)]
pub struct File {
    length: usize,
    path: Vec<String>,
}
