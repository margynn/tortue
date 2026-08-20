mod parse;

const PIECE_HASH_LEN: usize = 20;

pub type PieceHash = [u8; PIECE_HASH_LEN];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InfoHash([u8; PIECE_HASH_LEN]);

impl From<[u8; PIECE_HASH_LEN]> for InfoHash {
    fn from(bytes: [u8; PIECE_HASH_LEN]) -> Self {
        Self(bytes)
    }
}

impl AsRef<[u8]> for InfoHash {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("bencode parsing failed: {0}")]
    Bencode(#[from] crate::bencode::Error),

    #[error("invalid UTF-8 string")]
    InvalidUtf8,

    #[error("unexpected bencode type")]
    UnexpectedType,

    #[error("length value is negative")]
    NegativeLength,

    #[error("pieces data length is not a multiple of {PIECE_HASH_LEN}")]
    InvalidPiecesLength,
}

pub fn decode(data: &[u8]) -> Result<Metainfo, Error> {
    let root = crate::bencode::Bencode::decode(data)?;
    parse::parse(root)
}

#[derive(Debug, Clone)]
pub struct Metainfo {
    pub announce: Vec<String>,
    pub comment: Option<String>,
    pub created_by: Option<String>,
    pub created_at: Option<i64>,
    pub url_list: Option<Vec<String>>,
    pub name: String,
    pub hash: InfoHash,
    pub piece_length: u64,
    pub pieces: Vec<PieceHash>,
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
