use super::{Error, *};
use crate::bencode::Bencode;

/// Decodes bencode data into a `Metainfo` value.
///
/// # Errors
///
/// Returns an error if the input is not valid bencode format.
pub fn decode(data: &[u8]) -> Result<Metainfo, Error> {
    let decoded = crate::bencode::decode(data)?;

    let announce = get_bytes(&decoded, b"announce")?.to_vec();
    let info_bencode = decoded.get(b"info").ok_or(Error::InvalidDictKey)?;

    let name = get_bytes(info_bencode, b"name")?.to_vec();
    let piece_length = get_int(info_bencode, b"piece length")?;
    let pieces_bytes = get_bytes(info_bencode, b"pieces")?;
    let pieces = parse_pieces(pieces_bytes)?;

    let mode = match info_bencode.get(b"length") {
        Some(Bencode::Int(l)) => Mode::Single { length: *l as usize },
        Some(_) => return Err(Error::InvalidDictKey),
        None => parse_multi_file(info_bencode)?,
    };

    Ok(Metainfo { announce, info: InfoDictionary { name, piece_length, pieces, mode } })
}
fn get_bytes<'a>(dict: &'a Bencode, key: &[u8]) -> Result<&'a [u8], Error> {
    match dict.get(key) {
        Some(Bencode::Bytes(b)) => Ok(*b),
        _ => return Err(Error::InvalidDictKey),
    }
}

fn get_int<'a>(dict: &Bencode, key: &[u8]) -> Result<usize, Error> {
    match dict.get(key) {
        Some(Bencode::Int(n)) => Ok(*n as usize),
        _ => return Err(Error::InvalidDictKey),
    }
}

fn get_list<'a>(dict: &'a Bencode, key: &[u8]) -> Result<&'a Vec<Bencode<'a>>, Error> {
    match dict.get(key) {
        Some(Bencode::List(l)) => Ok(l),
        _ => return Err(Error::InvalidDictKey),
    }
}

fn parse_pieces(data: &[u8]) -> Result<Vec<[u8; SHA_LENGTH]>, Error> {
    if data.len() % SHA_LENGTH != 0 {
        return Err(Error::InvalidDictKey);
    }
    Ok(data
        .chunks(SHA_LENGTH)
        .map(|chunk| {
            let mut arr = [0u8; SHA_LENGTH];
            arr.copy_from_slice(chunk);
            arr
        })
        .collect())
}

fn parse_multi_file(info: &Bencode) -> Result<Mode, Error> {
    let files_bencode = get_list(info, b"files")?;
    let mut files = Vec::with_capacity(files_bencode.len());

    for file in files_bencode {
        let length = get_int(file, b"length")?;
        let path_list = get_list(file, b"path")?;
        let mut path = Vec::with_capacity(path_list.len());
        for p in path_list {
            match p {
                Bencode::Bytes(b) => path.push(b.to_vec()),
                _ => return Err(Error::InvalidDictKey),
            }
        }
        files.push(File { length, path });
    }

    Ok(Mode::Multiple { files })
}
