mod decode;

use std::collections::HashSet;

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

pub fn decode(data: &[u8]) -> Result<Metainfo, Error> {
    let root = crate::bencode::decode(data)?;
    println!("{}", root);
    decode::decode(root)
}

#[derive(Debug, Clone)]
pub struct Metainfo {
    pub announce: Vec<String>,
    pub comment: Option<String>,
    pub created_by: Option<String>,
    pub created_at: Option<i64>,
    pub url_list: Option<Vec<String>>,
    pub name: String,
    pub hash: [u8; SHA_LENGTH],
    pub piece_length: u64,
    pub pieces: Vec<[u8; SHA_LENGTH]>,
    pub mode: Mode,
}

#[derive(Debug, Clone)]
pub enum Mode {
    Single { length: u64 },
    Multiple { files: Vec<File> },
}

#[derive(Debug, Clone)]
pub struct File {
    pub length: u64,
    pub path: Vec<String>,
}

impl Metainfo {
    pub fn size(&self) -> u64 {
        match &self.mode {
            Mode::Single { length } => *length,
            Mode::Multiple { files } => files.iter().map(|f| f.length).sum(),
        }
    }
}
