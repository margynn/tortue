use std::collections::HashSet;

use sha1::{Digest, Sha1};

use super::{Error, File, InfoHash, Metainfo, Mode, PIECE_HASH_LEN, PieceHash};
use crate::bencode::Bencode;

pub(super) fn parse(root: Bencode<'_>) -> Result<Metainfo, Error> {
    let announce = parse_announces(&root)?;
    let info = root.get(b"info")?;
    let hash = info_hash(info)?;
    let (name, piece_length, pieces, mode) = parse_info(info)?;
    let comment = root.get_utf8(b"comment").ok();
    let created_by = root.get_utf8(b"created by").ok();
    let created_at = root.get_int(b"creation date").ok();
    let url_list = parse_url_list(&root);

    Ok(Metainfo {
        announce,
        name,
        hash,
        piece_length,
        pieces,
        mode,
        comment,
        created_by,
        created_at,
        url_list,
    })
}

fn parse_announces(root: &Bencode) -> Result<Vec<String>, Error> {
    let main_announce = root.get_utf8(b"announce")?;
    let tiers = root
        .get_list(b"announce-list")
        .map(parse_announce_list)
        .unwrap_or_default();

    let mut seen = HashSet::new();
    let mut announces = Vec::new();

    seen.insert(main_announce.clone());
    announces.push(main_announce);

    for tier in tiers {
        for url in tier {
            if seen.insert(url.clone()) {
                announces.push(url);
            }
        }
    }

    Ok(announces)
}

fn parse_url_list(root: &Bencode) -> Option<Vec<String>> {
    let list = root.get_list(b"url-list").ok()?;
    let v: Vec<String> = list
        .iter()
        .filter_map(|item| match item {
            Bencode::Bytes(bytes) => {
                std::str::from_utf8(bytes).ok().map(str::to_owned)
            },
            _ => None,
        })
        .collect();
    (!v.is_empty()).then_some(v)
}

#[allow(clippy::type_complexity)]
fn parse_info(
    info: &Bencode,
) -> Result<(String, u64, Vec<PieceHash>, Mode), Error> {
    let name = info.get_utf8(b"name")?;
    let piece_length = to_u64(info.get_int(b"piece length")?)?;
    let pieces = parse_pieces(info.get_bytes(b"pieces")?)?;

    let mode = match info.get(b"length") {
        Ok(Bencode::Int(length)) => Mode::Single { length: to_u64(*length)? },
        Ok(_) => return Err(Error::UnexpectedType),
        Err(_) => parse_multi_file(info)?,
    };

    Ok((name, piece_length, pieces, mode))
}

fn info_hash(info: &Bencode) -> Result<InfoHash, Error> {
    let encoded = info.encode();
    let digest = Sha1::digest(encoded);

    let mut bytes = [0u8; PIECE_HASH_LEN];
    bytes.copy_from_slice(&digest);
    Ok(InfoHash::from(bytes))
}

fn parse_announce_list(tiers: &[Bencode]) -> Vec<Vec<String>> {
    tiers
        .iter()
        .filter_map(|tier| match tier {
            Bencode::List(urls) => {
                let urls = urls
                    .iter()
                    .filter_map(|url| match url {
                        Bencode::Bytes(bytes) => {
                            std::str::from_utf8(bytes).ok().map(str::to_owned)
                        },
                        _ => None,
                    })
                    .collect::<Vec<_>>();

                (!urls.is_empty()).then_some(urls)
            },
            _ => None,
        })
        .collect()
}

fn parse_pieces(data: &[u8]) -> Result<Vec<PieceHash>, Error> {
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

fn parse_multi_file(info: &Bencode) -> Result<Mode, Error> {
    let files = info
        .get_list(b"files")?
        .iter()
        .map(parse_file)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Mode::Multiple { files })
}

fn parse_file(file: &Bencode) -> Result<File, Error> {
    let length = to_u64(file.get_int(b"length")?)?;
    let path = file
        .get_list(b"path")?
        .iter()
        .map(|component| match component {
            Bencode::Bytes(bytes) => std::str::from_utf8(bytes)
                .map(str::to_owned)
                .map_err(|_| Error::InvalidUtf8),
            _ => Err(Error::UnexpectedType),
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(File { length, path })
}

fn to_u64(value: i64) -> Result<u64, Error> {
    u64::try_from(value).map_err(|_| Error::NegativeLength)
}
