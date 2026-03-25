use crate::bencode::{Bencode, Error};

/// Encode a Bencode value to bytes
pub fn encode(bencode: &Bencode) -> Result<Vec<u8>, Error> {
    let mut buffer = Vec::new();
    
    match bencode {
        Bencode::Int(n) => {
            let s = n.to_string();
            buffer.extend_from_slice(s.as_bytes());
            buffer.push(b'i');
            buffer.extend_from_slice(&s.into_bytes());
            buffer.push(b'e');
        }
        Bencode::Bytes(bytes) => {
            let len = bytes.len();
            let len_str = len.to_string();
            buffer.extend_from_slice(len_str.as_bytes());
            buffer.push(b':');
            buffer.extend_from_slice(bytes);
        }
        Bencode::List(items) => {
            buffer.push(b'l');
            for item in items {
                encode(item)?;
            }
            buffer.push(b'e');
        }
        Bencode::Dict(entries) => {
            buffer.push(b'd');
            for (key, value) in entries {
                encode(key)?;
                encode(value)?;
            }
            buffer.push(b'e');
        }
    }
    
    Ok(buffer)
}

/// Encode an integer to bencode format
pub fn encode_int(n: i64) -> Result<Vec<u8>, Error> {
    let s = n.to_string();
    let mut buffer = Vec::new();
    buffer.extend_from_slice(s.as_bytes());
    buffer.push(b'i');
    buffer.extend_from_slice(&s.into_bytes());
    buffer.push(b'e');
    Ok(buffer)
}

/// Encode bytes to bencode format
pub fn encode_bytes(bytes: &[u8]) -> Result<Vec<u8>, Error> {
    let len = bytes.len();
    let len_str = len.to_string();
    let mut buffer = Vec::new();
    buffer.extend_from_slice(len_str.as_bytes());
    buffer.push(b':');
    buffer.extend_from_slice(bytes);
    Ok(buffer)
}

/// Encode a string to bencode format (UTF-8 encoded bytes)
pub fn encode_string(s: &str) -> Result<Vec<u8>, Error> {
    let bytes = s.as_bytes();
    encode_bytes(bytes)
}

/// Encode a list of Bencode values
pub fn encode_list(items: &[Bencode]) -> Result<Vec<u8>, Error> {
    let mut buffer = Vec::new();
    buffer.push(b'l');
    for item in items {
        encode(item)?;
    }
    buffer.push(b'e');
    Ok(buffer)
}

/// Encode a dictionary of Bencode values
pub fn encode_dict(entries: &[(&[u8], Bencode)]) -> Result<Vec<u8>, Error> {
    let mut buffer = Vec::new();
    buffer.push(b'd');
    for (key, value) in entries {
        encode(key)?;
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
        let encoded = encode_int(42).unwrap();
        assert_eq!(encoded, b"i42e");
    }

    #[test]
    fn test_encode_bytes() {
        let encoded = encode_bytes(b"hello").unwrap();
        assert_eq!(encoded, b"5:hello");
    }

    #[test]
    fn test_encode_string() {
        let encoded = encode_string("world").unwrap();
        assert_eq!(encoded, b"5:world");
    }

    #[test]
    fn test_encode_list() {
        let items = vec![
            Bencode::Int(1),
            Bencode::Bytes(b"two"),
        ];
        let encoded = encode_list(&items).unwrap();
        assert_eq!(encoded, b"li1e3:twoe");
    }

    #[test]
    fn test_encode_dict() {
        let entries = vec![
            (b"name".as_ref(), Bencode::Bytes(b"John")),
            (b"age".as_ref(), Bencode::Int(25)),
        ];
        let encoded = encode_dict(&entries).unwrap();
        assert_eq!(encoded, b"d4:name5:Johni25ee");
    }

    #[test]
    fn test_encode_complex() {
        let dict = vec![
            (b"list".as_ref(), Bencode::List(vec![
                Bencode::Int(1),
                Bencode::Int(2),
            ])),
            (b"value".as_ref(), Bencode::Bytes(b"test")),
        ];
        let encoded = encode_dict(&dict).unwrap();
        assert_eq!(encoded, b"d4:listli1ei2ee5:value4:testee");
    }
}
