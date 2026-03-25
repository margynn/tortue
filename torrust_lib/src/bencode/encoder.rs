//! Bencode encoding functions.
//!
//! This module provides functions to encode Rust data types into Bencode format.

use crate::bencode::{Bencode, Error};

/// Encode a Bencode value to bytes
pub fn encode(bencode: &Bencode) -> Result<Vec<u8>, Error> {
    let buffer = match bencode {
        Bencode::Int(n) => encode_int(*n),
        Bencode::Bytes(bytes) => encode_bytes(bytes),
        Bencode::List(items) => encode_list(items),
        Bencode::Dict(entries) => encode_dict(entries),
    }?;
    Ok(buffer)
}

/// Encode an integer to bencode format
fn encode_int(n: i64) -> Result<Vec<u8>, Error> {
    let s = n.to_string();
    let mut buffer = Vec::new();
    buffer.push(b'i');
    buffer.extend_from_slice(s.as_bytes());
    buffer.push(b'e');
    Ok(buffer)
}

/// Encode bytes to bencode format
fn encode_bytes(bytes: &[u8]) -> Result<Vec<u8>, Error> {
    let len_str = bytes.len().to_string();
    let mut buffer = Vec::new();
    buffer.extend_from_slice(len_str.as_bytes());
    buffer.push(b':');
    buffer.extend_from_slice(bytes);
    Ok(buffer)
}

/// Encode a list of Bencode values
fn encode_list(items: &Vec<Bencode>) -> Result<Vec<u8>, Error> {
    let mut buffer = Vec::new();
    buffer.push(b'l');
    for item in items {
        buffer.extend(encode(item)?);
    }
    buffer.push(b'e');
    Ok(buffer)
}

/// Encode a dictionary of Bencode values
fn encode_dict(entries: &Vec<(&[u8], Bencode)>) -> Result<Vec<u8>, Error> {
    let mut buffer = Vec::new();
    buffer.push(b'd');
    for (key, value) in entries {
        buffer.extend(encode_bytes(key)?);
        buffer.extend(encode(value)?);
    }
    buffer.push(b'e');
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_int() {
        let result = encode_int(42);
        assert!(result.is_ok());
        let encoded = result.unwrap();
        assert_eq!(encoded, b"i42e");
    }

    #[test]
    fn test_encode_negative_int() {
        let result = encode_int(-10);
        assert!(result.is_ok());
        let encoded = result.unwrap();
        assert_eq!(encoded, b"i-10e");
    }

    #[test]
    fn test_encode_bytes() {
        let bytes = b"hello";
        let result = encode_bytes(bytes);
        assert!(result.is_ok());
        let encoded = result.unwrap();
        assert_eq!(encoded, b"5:hello");
    }

    #[test]
    fn test_encode_empty_bytes() {
        let bytes: &[u8] = &[];
        let result = encode_bytes(bytes);
        assert!(result.is_ok());
        let encoded = result.unwrap();
        assert_eq!(encoded, b"0:");
    }

    #[test]
    fn test_encode_list() {
        let items = vec![Bencode::Int(1), Bencode::Bytes(b"test"), Bencode::List(vec![])];
        let result = encode_list(&items);
        assert!(result.is_ok());
        let encoded = result.unwrap();
        assert_eq!(encoded, b"li1e4:testlee");
    }

    #[test]
    fn test_encode_dict() {
        let entries = vec![
            (b"name".as_slice(), Bencode::Bytes(b"John")),
            (b"age".as_slice(), Bencode::Int(30)),
        ];
        let result = encode_dict(&entries);
        assert!(result.is_ok());
        let encoded = result.unwrap();
        assert_eq!(encoded, b"d4:name4:John3:agei30ee");
    }

    #[test]
    fn test_encode() {
        let bencode = Bencode::Int(123);
        let result = encode(&bencode);
        assert!(result.is_ok());
        let encoded = result.unwrap();
        assert_eq!(encoded, b"i123e");
    }

    #[test]
    fn test_encode_bytes_with_special_chars() {
        let bytes = b"hello world!";
        let result = encode_bytes(bytes);
        assert!(result.is_ok());
        let encoded = result.unwrap();
        assert_eq!(encoded, b"12:hello world!");
    }

    #[test]
    fn test_encode_list_with_mixed_types() {
        let items =
            vec![Bencode::Int(0), Bencode::Bytes(b"empty"), Bencode::List(vec![Bencode::Int(1)])];
        let result = encode_list(&items);
        assert!(result.is_ok());
        let encoded = result.unwrap();
        assert_eq!(encoded, b"li0e5:emptyli1eee");
    }

    #[test]
    fn test_encode_dict_with_empty_values() {
        let entries =
            vec![(b"a".as_slice(), Bencode::Int(0)), (b"b".as_slice(), Bencode::Bytes(b""))];
        let result = encode_dict(&entries);
        assert!(result.is_ok());
        let encoded = result.unwrap();
        assert_eq!(encoded, b"d1:ai0e1:b0:e");
    }
}
