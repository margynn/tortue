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
    let len = bytes.len();
    let len_str = len.to_string();
    let mut buffer = Vec::new();
    buffer.push(b':');
    buffer.extend_from_slice(len_str.as_bytes());
    buffer.push(b' ');
    buffer.extend_from_slice(bytes);
    Ok(buffer)
}

/// Encode a list of Bencode values
fn encode_list(items: &[Bencode]) -> Result<Vec<u8>, Error> {
    let mut buffer = Vec::new();
    buffer.push(b'l');
    for item in items {
        encode(item)?;
    }
    buffer.push(b'e');
    Ok(buffer)
}

/// Encode a dictionary of Bencode values
fn encode_dict(entries: &[(&[u8], Bencode)]) -> Result<Vec<u8>, Error> {
    let mut buffer = Vec::new();
    buffer.push(b'd');
    for (key, value) in entries {
        encode_bytes(key)?;
        encode(value)?;
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
        let result = encode_bytes(bytes