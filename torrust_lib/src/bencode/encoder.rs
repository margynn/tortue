use std::collections::BTreeMap;

use crate::bencode::Bencode;

pub(super) fn encode_into(buf: &mut Vec<u8>, value: &Bencode) {
    match value {
        Bencode::Int(n) => encode_int(buf, *n),
        Bencode::Bytes(bytes) => encode_bytes(buf, bytes),
        Bencode::List(items) => encode_list(buf, items),
        Bencode::Dict(entries) => encode_dict(buf, entries),
    }
}

fn encode_int(buf: &mut Vec<u8>, n: i64) {
    let s = n.to_string();
    buf.push(b'i');
    buf.extend_from_slice(s.as_bytes());
    buf.push(b'e');
}

fn encode_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    let len_str = bytes.len().to_string();
    buf.extend_from_slice(len_str.as_bytes());
    buf.push(b':');
    buf.extend_from_slice(bytes);
}

fn encode_list(buf: &mut Vec<u8>, items: &Vec<Bencode>) {
    buf.push(b'l');
    for item in items {
        encode_into(buf, item);
    }
    buf.push(b'e');
}

fn encode_dict(buf: &mut Vec<u8>, entries: &BTreeMap<&[u8], Bencode>) {
    buf.push(b'd');
    for (key, value) in entries {
        encode_bytes(buf, key);
        encode_into(buf, value);
    }
    buf.push(b'e');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_int() {
        let mut buf = Vec::new();
        encode_int(&mut buf, 42);
        assert_eq!(buf, b"i42e");
    }

    #[test]
    fn test_encode_negative_int() {
        let mut buf = Vec::new();
        encode_int(&mut buf, -10);
        assert_eq!(buf, b"i-10e");
    }

    #[test]
    fn test_encode_bytes() {
        let mut buf = Vec::new();
        encode_bytes(&mut buf, b"hello");
        assert_eq!(buf, b"5:hello");
    }

    #[test]
    fn test_encode_empty_bytes() {
        let mut buf = Vec::new();
        encode_bytes(&mut buf, b"");
        assert_eq!(buf, b"0:");
    }

    #[test]
    fn test_encode_list() {
        let mut buf = Vec::new();
        let items = vec![
            Bencode::Int(1),
            Bencode::Bytes(b"test"),
            Bencode::List(vec![]),
        ];
        encode_list(&mut buf, &items);
        assert_eq!(buf, b"li1e4:testlee");
    }

    #[test]
    fn test_encode_dict() {
        let mut buf = Vec::new();
        let mut entries = BTreeMap::new();
        entries.insert(b"name".as_slice(), Bencode::Bytes(b"John"));
        entries.insert(b"age".as_slice(), Bencode::Int(30));
        encode_dict(&mut buf, &entries);
        assert_eq!(buf, b"d4:name4:John3:agei30ee");
    }

    #[test]
    fn test_encode_bytes_with_special_chars() {
        let mut buf = Vec::new();
        encode_bytes(&mut buf, b"hello world!");
        assert_eq!(buf, b"12:hello world!");
    }

    #[test]
    fn test_encode_list_with_mixed_types() {
        let mut buf = Vec::new();
        let items = vec![
            Bencode::Int(0),
            Bencode::Bytes(b"empty"),
            Bencode::List(vec![Bencode::Int(1)]),
        ];
        encode_list(&mut buf, &items);
        assert_eq!(buf, b"li0e5:emptyli1eee");
    }

    #[test]
    fn test_encode_dict_with_empty_values() {
        let mut buf = Vec::new();
        let mut entries = BTreeMap::new();
        entries.insert(b"a".as_slice(), Bencode::Int(0));
        entries.insert(b"b".as_slice(), Bencode::Bytes(b""));
        encode_dict(&mut buf, &entries);
        assert_eq!(buf, b"d1:ai0e1:b0:e");
    }
}
