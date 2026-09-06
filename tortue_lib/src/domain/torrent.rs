use super::bencode::{Bencode, Error as BencodeError};
use sha1::{Digest, Sha1};
use std::collections::HashSet;

pub type PieceHash = [u8; PIECE_HASH_LEN];
pub const PIECE_HASH_LEN: usize = 20;

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

#[derive(Debug, Clone)]
pub struct Metainfo {
    pub name: String,
    pub announce: Vec<String>,
    pub comment: Option<String>,
    pub created_by: Option<String>,
    pub created_at: Option<i64>,
    pub url_list: Option<Vec<String>>,
    pub info_bytes: Vec<u8>,
    pub hash: InfoHash,
    pub piece_length: usize,
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
    const METADATA_PIECE_SIZE: usize = 16_384; // 16KB

    /// content size in bytes of the torrent content
    pub fn total_size(&self) -> u64 {
        match &self.mode {
            Mode::Single { length } => *length,
            Mode::Multiple { files } => files.iter().map(|f| f.length).sum(),
        }
    }

    pub fn info_bytes_block(&self, piece: usize) -> Vec<u8> {
        let start = piece as usize * Self::METADATA_PIECE_SIZE;
        if start >= self.info_bytes.len() {
            return vec![];
        }
        self.info_bytes[start..]
            .chunks(Self::METADATA_PIECE_SIZE)
            .next()
            .unwrap_or_default()
            .to_vec()
    }
}

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

impl TryFrom<&[u8]> for Metainfo {
    type Error = Error;

    fn try_from(data: &[u8]) -> Result<Self> {
        let root = Bencode::decode(data)?;
        let announce = parse_announces(&root)?;
        let info = root.get(b"info")?;
        let (hash, info_bytes) = info_hash(info)?;
        let (name, piece_length, pieces, mode) = parse_info(info)?;

        Ok(Metainfo {
            announce,
            name,
            hash,
            piece_length,
            pieces,
            mode,
            info_bytes,
            comment: root.get_utf8(b"comment").ok(),
            created_by: root.get_utf8(b"created by").ok(),
            created_at: root.get_int(b"creation date").ok(),
            url_list: parse_url_list(&root),
        })
    }
}

impl TryFrom<&Bencode<'_>> for File {
    type Error = Error;

    fn try_from(b: &Bencode<'_>) -> Result<Self> {
        let length = to_u64(b.get_int(b"length")?)?;
        let path = b
            .get_list(b"path")?
            .iter()
            .map(|c| bytes_to_str(c).ok_or(Error::InvalidUtf8))
            .collect::<Result<Vec<_>>>()?;
        Ok(File { length, path })
    }
}

fn bytes_to_str(b: &Bencode) -> Option<String> {
    match b {
        Bencode::Bytes(bytes) => std::str::from_utf8(bytes).ok().map(str::to_owned),
        _ => None,
    }
}

fn parse_announces(root: &Bencode) -> Result<Vec<String>> {
    let main_announce = root.get_utf8(b"announce")?;
    let tiers = root
        .get_list(b"announce-list")
        .map(parse_announce_list)
        .unwrap_or_default();

    let mut seen = HashSet::new();
    let mut announces = vec![main_announce.clone()];
    seen.insert(main_announce);

    for url in tiers.into_iter().flatten() {
        if seen.insert(url.clone()) {
            announces.push(url);
        }
    }

    Ok(announces)
}

fn parse_url_list(root: &Bencode) -> Option<Vec<String>> {
    let list = root.get_list(b"url-list").ok()?;
    let v: Vec<String> = list.iter().filter_map(bytes_to_str).collect();
    (!v.is_empty()).then_some(v)
}

fn parse_announce_list(tiers: &[Bencode]) -> Vec<Vec<String>> {
    tiers
        .iter()
        .filter_map(|tier| match tier {
            Bencode::List(urls) => {
                let urls: Vec<String> = urls.iter().filter_map(bytes_to_str).collect();
                (!urls.is_empty()).then_some(urls)
            },
            _ => None,
        })
        .collect()
}

fn parse_info(info: &Bencode) -> Result<(String, usize, Vec<PieceHash>, Mode)> {
    let name = info.get_utf8(b"name")?;
    let piece_length = to_usize(info.get_int(b"piece length")?)?;
    let pieces = parse_pieces(info.get_bytes(b"pieces")?)?;

    let mode = match info.get(b"length") {
        Ok(Bencode::Int(length)) => Mode::Single {
            length: to_u64(*length)?,
        },
        Ok(_) => return Err(Error::UnexpectedType),
        Err(_) => {
            let files = info
                .get_list(b"files")?
                .iter()
                .map(File::try_from)
                .collect::<Result<Vec<_>>>()?;
            Mode::Multiple { files }
        },
    };

    Ok((name, piece_length, pieces, mode))
}

fn info_hash(info: &Bencode) -> Result<(InfoHash, Vec<u8>)> {
    let encoded_info = info.encode();
    let digest = Sha1::digest(&encoded_info);
    let mut bytes = [0u8; PIECE_HASH_LEN];
    bytes.copy_from_slice(&digest);
    Ok((InfoHash::from(bytes), encoded_info))
}

fn parse_pieces(data: &[u8]) -> Result<Vec<PieceHash>> {
    if !data.len().is_multiple_of(PIECE_HASH_LEN) {
        return Err(Error::InvalidPiecesLength);
    }
    Ok(data
        .chunks_exact(PIECE_HASH_LEN)
        .map(|chunk| {
            let mut piece = [0u8; PIECE_HASH_LEN];
            piece.copy_from_slice(chunk);
            piece
        })
        .collect())
}

fn to_u64(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::NegativeLength)
}

fn to_usize(value: i64) -> Result<usize> {
    usize::try_from(value).map_err(|_| Error::NegativeLength)
}
