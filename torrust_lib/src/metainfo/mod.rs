pub mod decode;
const SHA_LENGTH: usize = 20;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("bencode parsing failed: {0}")]
    Bencode(#[from] crate::bencode::Error),
    #[error("invalid UTF-8 in announce")]
    InvalidUtf8Announce,
    #[error("invalid UTF-8 in name")]
    InvalidUtf8Name,
    #[error("invalid dictionary key")]
    InvalidDictKey,
}

pub struct Metainfo {
    announce: Vec<u8>,
    info: InfoDictionary,
}

pub struct InfoDictionary {
    name: Vec<u8>,
    piece_length: usize,
    pieces: Vec<[u8; SHA_LENGTH]>,
    mode: Mode,
}

pub enum Mode {
    Single { length: usize },
    Multiple { files: Vec<File> },
}

pub struct File {
    length: usize,
    path: Vec<Vec<u8>>,
}

impl Metainfo {
    fn announce_str(&self) -> Option<&str> {
        std::str::from_utf8(self.announce.as_ref()).ok()
    }
}

impl InfoDictionary {
    fn name_str(&self) -> Option<&str> {
        std::str::from_utf8(self.name.as_ref()).ok()
    }
}

impl File {
    fn path_strs(&self) -> Vec<&str> {
        self.path.iter().filter_map(|p| std::str::from_utf8(p).ok()).collect()
    }
}
