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
    decode::decode(root)
}

#[derive(Debug, Clone)]
pub struct Metainfo {
    announce: String,
    announce_list: Vec<Vec<String>>,
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

impl Metainfo {
    pub fn trackers(&self) -> Vec<String> {
        let cap = 1 + self.announce_list.iter().map(Vec::len).sum::<usize>();
        let mut trackers = HashSet::with_capacity(cap);
        trackers.insert(self.announce.clone());
        for tier in &self.announce_list {
            for tracker in tier {
                trackers.insert(tracker.clone());
            }
        }
        trackers.into_iter().collect()
    }
}
