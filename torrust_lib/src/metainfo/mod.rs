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
    pub announce: String,
    pub name: String,
    pub hash: [u8; SHA_LENGTH],
    pub piece_length: usize,
    pub pieces: Vec<[u8; SHA_LENGTH]>,
    pub mode: Mode,
}

#[derive(Debug, Clone)]
pub enum Mode {
    Single { length: usize },
    Multiple { files: Vec<File> },
}

#[derive(Debug, Clone)]
pub struct File {
    pub length: usize,
    pub path: Vec<String>,
}
