use super::{Error, *};
use crate::bencode::Bencode;
use sha1::{Digest, Sha1};

/// Decodes bencode data into a `Metainfo` value.
///
/// # Errors
///
/// Returns an error if the input is not valid bencode format.
pub fn decode(data: &[u8]) -> Result<Metainfo, Error> {
    let decoded = crate::bencode::decode(data)?;

    let announce = decoded.get_bytes(b"announce")?.to_vec();
    let info = decoded.get(b"info").ok_or(Error::InvalidDictKey)?;

    // TODO: re-encode info and then hash it
    let reencoded_info = vec![];
    let mut info_hash: [u8; 20] = [0u8; 20];
    info_hash.copy_from_slice(&Sha1::digest(reencoded_info));

    let name = info.get_bytes(b"name")?.to_vec();
    let piece_length = info.get_int(b"piece length")?;
    let pieces_bytes = info.get_bytes(b"pieces")?;
    let pieces = parse_pieces(pieces_bytes)?;

    let mode = match info.get(b"length") {
        Some(Bencode::Int(l)) => Mode::Single { length: *l as usize },
        Some(_) => return Err(Error::InvalidDictKey),
        None => parse_multi_file(info)?,
    };

    Ok(Metainfo {
        announce,
        info: InfoDictionary { hash: info_hash, name, piece_length, pieces, mode },
    })
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
    let files_bencode = info.get_list(b"files")?;
    let mut files = Vec::with_capacity(files_bencode.len());

    for file in files_bencode {
        let length = file.get_int(b"length")?;
        let path_list = file.get_list(b"path")?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sha_hash(byte: u8) -> [u8; SHA_LENGTH] {
        [byte; SHA_LENGTH]
    }

    #[test]
    fn test_decode_single_file() {
        let data = b"d8:announce8:test_url4:infod6:lengthi100e4:name9:test_file12:piece lengthi16384e6:pieces20:12345678901234567890ee";
        let metainfo = decode(data).unwrap();
        assert_eq!(metainfo.announce, b"test_url");
        assert_eq!(metainfo.info.name, b"test_file");
        assert_eq!(metainfo.info.piece_length, 16384);
        assert_eq!(metainfo.info.pieces.len(), 1);
        match &metainfo.info.mode {
            Mode::Single { length } => assert_eq!(*length, 100),
            Mode::Multiple { .. } => panic!("expected single-file mode"),
        }
    }

    #[test]
    fn test_decode_multi_file() {
        let data = b"d8:announce8:test_url4:infod5:filesld6:lengthi100e4:pathl5:file1eee4:name4:test12:piece lengthi16384e6:pieces20:12345678901234567890ee";
        let metainfo = decode(data).unwrap();
        assert_eq!(metainfo.announce, b"test_url");
        assert_eq!(metainfo.info.name, b"test");
        match &metainfo.info.mode {
            Mode::Multiple { files } => {
                assert_eq!(files.len(), 1);
                assert_eq!(files[0].length, 100);
                assert_eq!(files[0].path.len(), 1);
                assert_eq!(files[0].path[0], b"file1");
            },
            Mode::Single { .. } => panic!("expected multi-file mode"),
        }
    }

    #[test]
    fn test_decode_missing_announce() {
        let data = b"d4:infod4:name4:test12:piece lengthi16384e6:pieces20:12345678901234567890ee";
        let err = decode(data).unwrap_err();
        assert!(matches!(err, Error::Bencode(crate::bencode::Error::InvalidDictKey)));
    }

    #[test]
    fn test_decode_missing_info() {
        let data = b"d8:announce9:test_urlee";
        let err = decode(data).unwrap_err();
        assert!(matches!(err, Error::InvalidDictKey));
    }

    #[test]
    fn test_decode_missing_name() {
        let data =
            b"d8:announce9:test_url4:infod12:piece lengthi16384e6:pieces20:12345678901234567890ee";
        let err = decode(data).unwrap_err();
        assert!(matches!(err, Error::Bencode(crate::bencode::Error::InvalidDictKey)));
    }

    #[test]
    fn test_parse_pieces_valid() {
        let data = &[1; 40];
        let pieces = parse_pieces(data).unwrap();
        assert_eq!(pieces.len(), 2);
        assert_eq!(pieces[0], make_sha_hash(1));
        assert_eq!(pieces[1], make_sha_hash(1));
    }

    #[test]
    fn test_parse_pieces_invalid_length() {
        let data = &[1; 19];
        let err = parse_pieces(data).unwrap_err();
        assert!(matches!(err, Error::InvalidDictKey));
    }

    #[test]
    fn test_parse_pieces_empty() {
        let data = &[];
        let pieces = parse_pieces(data).unwrap();
        assert_eq!(pieces.len(), 0);
    }
}
