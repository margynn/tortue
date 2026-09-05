use std::collections::HashSet;

use sha1::{Digest, Sha1};

use super::bencode::{Bencode, Error as BencodeError};
use crate::domain::torrent::{File, InfoHash, Metainfo, Mode, PIECE_HASH_LEN, PieceHash};

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
        let hash = info_hash(info)?;
        let (name, piece_length, pieces, mode) = parse_info(info)?;

        Ok(Metainfo {
            announce,
            name,
            hash,
            piece_length,
            pieces,
            mode,
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

fn info_hash(info: &Bencode) -> Result<InfoHash> {
    let digest = Sha1::digest(info.encode());
    let mut bytes = [0u8; PIECE_HASH_LEN];
    bytes.copy_from_slice(&digest);
    Ok(InfoHash::from(bytes))
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
