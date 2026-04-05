use std::collections::BTreeMap;

use crate::bencode::encoder::encode_into;
use crate::bencode::{Error, Result, decoder};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bencode<'a> {
    Int(i64),
    Bytes(&'a [u8]),
    List(Vec<Bencode<'a>>),
    Dict(BTreeMap<&'a [u8], Bencode<'a>>),
}

impl<'a> Bencode<'a> {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_into(&mut buf, self);
        buf
    }

    pub fn decode(data: &'a [u8]) -> Result<Self> {
        decoder::decode(data)
    }

    pub fn get(&self, key: &[u8]) -> Result<&Bencode<'a>> {
        match self {
            Bencode::Dict(entries) => {
                entries.get(key).ok_or(Error::KeyNotFound)
            },
            _ => Err(Error::TypeMismatch),
        }
    }

    pub fn get_utf8(&self, key: &[u8]) -> Result<String> {
        let bytes = self.get_bytes(key)?;
        let s = std::str::from_utf8(bytes).map_err(|_| Error::InvalidString)?;
        Ok(s.to_owned())
    }

    pub fn get_bytes(&self, key: &[u8]) -> Result<&'a [u8]> {
        match self.get(key) {
            Ok(Bencode::Bytes(bytes)) => Ok(bytes),
            _ => Err(Error::TypeMismatch),
        }
    }

    pub fn get_int(&self, key: &[u8]) -> Result<i64> {
        match self.get(key) {
            Ok(Bencode::Int(n)) => Ok(*n),
            _ => Err(Error::TypeMismatch),
        }
    }

    pub fn get_list(&self, key: &[u8]) -> Result<&[Bencode<'a>]> {
        match self.get(key) {
            Ok(Bencode::List(list)) => Ok(list.as_slice()),
            _ => Err(Error::TypeMismatch),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Bencode;

    #[test]
    fn test_get_existing_key() {
        let dict = Bencode::Dict(vec![
            (b"foo".as_ref(), Bencode::Int(42)),
            (b"bar".as_ref(), Bencode::Bytes(b"hello")),
        ]);
        assert_eq!(dict.get(b"foo"), Some(&Bencode::Int(42)));
        assert_eq!(dict.get(b"bar"), Some(&Bencode::Bytes(b"hello")));
    }

    #[test]
    fn test_get_nonexistent_key() {
        let dict = Bencode::Dict(vec![
            (b"foo".as_ref(), Bencode::Int(42)),
            (b"bar".as_ref(), Bencode::Bytes(b"hello")),
        ]);
        assert_eq!(dict.get(b"baz"), None);
    }

    #[test]
    fn test_get_on_non_dict() {
        let integer = Bencode::Int(42);
        let list = Bencode::List(vec![Bencode::Int(1)]);
        assert_eq!(integer.get(b"foo"), None);
        assert_eq!(list.get(b"foo"), None);
    }

    #[test]
    fn test_get_empty_dict() {
        let empty_dict = Bencode::Dict(vec![]);
        assert_eq!(empty_dict.get(b"foo"), None);
    }

    #[test]
    fn test_get_bytes_valid() {
        let bencode = super::Bencode::decode(b"d4:test5:valueee").unwrap();
        let value = bencode.get_bytes(b"test").unwrap();
        assert_eq!(value, b"value");
    }

    #[test]
    fn test_get_bytes_missing_key() {
        let bencode = super::Bencode::decode(b"d4:test5:valueee").unwrap();
        let err = bencode.get_bytes(b"missing").unwrap_err();
        assert!(matches!(err, super::Error::InvalidDictKey));
    }

    #[test]
    fn test_get_bytes_wrong_type() {
        let bencode = super::Bencode::decode(b"d4:testi42eee").unwrap();
        let err = bencode.get_bytes(b"test").unwrap_err();
        assert!(matches!(err, super::Error::InvalidDictKey));
    }

    #[test]
    fn test_get_int_valid() {
        let bencode = super::Bencode::decode(b"d4:testi42eee").unwrap();
        let value = bencode.get_int(b"test").unwrap();
        assert_eq!(value, 42);
    }

    #[test]
    fn test_get_int_missing_key() {
        let bencode = super::Bencode::decode(b"d4:testi42eee").unwrap();
        let err = bencode.get_int(b"missing").unwrap_err();
        assert!(matches!(err, super::Error::InvalidDictKey));
    }

    #[test]
    fn test_get_int_wrong_type() {
        let bencode = super::Bencode::decode(b"d4:test5:valueee").unwrap();
        let err = bencode.get_int(b"test").unwrap_err();
        assert!(matches!(err, super::Error::InvalidDictKey));
    }

    #[test]
    fn test_get_list_valid() {
        let bencode = super::Bencode::decode(b"d4:testl5:valueee").unwrap();
        let list = bencode.get_list(b"test").unwrap();
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn test_get_list_missing_key() {
        let bencode = super::Bencode::decode(b"d4:testl5:valueee").unwrap();
        let err = bencode.get_list(b"missing").unwrap_err();
        assert!(matches!(err, super::Error::InvalidDictKey));
    }

    #[test]
    fn test_get_list_wrong_type() {
        let bencode = super::Bencode::decode(b"d4:test5:valueee").unwrap();
        let err = bencode.get_list(b"test").unwrap_err();
        assert!(matches!(err, super::Error::InvalidDictKey));
    }
}
