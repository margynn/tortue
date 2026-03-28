use sha1::{Digest, Sha1};

use super::{Error, *};
use crate::bencode::Bencode;

pub(super) fn decode(root: Bencode<'_>) -> Result<Metainfo, Error> {
    // let root: Bencode<'_> = crate::bencode::decode(data)?;

    let announce = get_utf8(&root, b"announce")?;
    let announce_list = root
        .get_list(b"announce-list")
        .map(parse_announce_list)
        .unwrap_or_default();

    let info = root.get(b"info").ok_or(Error::InvalidDictKey)?;
    let hash = info_hash(info)?;
    let (name, piece_length, pieces, mode) = parse_info(info)?;

    Ok(Metainfo {
        announce,
        announce_list,
        name,
        hash,
        piece_length,
        pieces,
        mode,
    })
}

fn parse_info(
    info: &Bencode,
) -> Result<(String, u64, Vec<[u8; SHA_LENGTH]>, Mode), Error> {
    let name = get_utf8(info, b"name")?;
    let piece_length = to_u64(info.get_int(b"piece length")?)?;
    let pieces = parse_pieces(info.get_bytes(b"pieces")?)?;

    let mode = match info.get(b"length") {
        Some(Bencode::Int(length)) => {
            let length = to_u64(*length)?;
            Mode::Single { length }
        },
        Some(_) => return Err(Error::InvalidDictKey),
        None => parse_multi_file(info)?,
    };

    Ok((name, piece_length, pieces, mode))
}

fn info_hash(info: &Bencode) -> Result<[u8; 20], Error> {
    let encoded = info.encode()?;
    let digest = Sha1::digest(encoded);

    let mut hash = [0u8; 20];
    hash.copy_from_slice(&digest);
    Ok(hash)
}

fn get_utf8(bencode: &Bencode, key: &[u8]) -> Result<String, Error> {
    let bytes = bencode.get_bytes(key)?;
    let s = std::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8String)?;
    Ok(s.to_owned())
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

fn parse_pieces(data: &[u8]) -> Result<Vec<[u8; SHA_LENGTH]>, Error> {
    if data.len() % SHA_LENGTH != 0 {
        return Err(Error::InvalidDictKey);
    }

    Ok(data
        .chunks_exact(SHA_LENGTH)
        .map(|chunk| {
            let mut piece = [0u8; SHA_LENGTH];
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
                .map_err(|_| Error::InvalidUtf8String),
            _ => Err(Error::InvalidDictKey),
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(File { length, path })
}

fn to_u64(value: i64) -> Result<u64, Error> {
    u64::try_from(value).map_err(|_| Error::InvalidDictKey)
}
