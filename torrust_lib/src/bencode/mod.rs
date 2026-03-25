mod decoder;
mod encoder;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("expected byte {expected:#x}, found {found:#x}")]
    UnexpectedByte { expected: u8, found: u8 },
    #[error("invalid token: {0:#x}")]
    InvalidToken(u8),
    #[error("invalid integer")]
    InvalidInteger,
    #[error("invalid string length")]
    InvalidStringLength,
    #[error("invalid dictionary key")]
    InvalidDictKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bencode<'a> {
    Int(i64),
    Bytes(&'a [u8]),
    List(Vec<Bencode<'a>>),
    Dict(Vec<(&'a [u8], Bencode<'a>)>),
}

/// Decodes bencode data into a `Bencode` value.
///
/// # Errors
///
/// Returns an error if the input is not valid bencode format.
pub fn decode(data: &[u8]) -> Result<Bencode<'_>, Error> {
    decoder::Decoder::new(data).parse()
}

impl<'a> Bencode<'a> {
    #[must_use]
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        encoder::encode(self)
    }

    /// Lookup a key in a Bencode dictionary
    pub fn get(&self, key: &[u8]) -> Option<&Bencode<'a>> {
        match self {
            Bencode::Dict(entries) => {
                // linear scan; keys are byte slices
                entries.iter().find(|(k, _)| *k == key).map(|(_, v)| v)
            },
            _ => None,
        }
    }

    /// Get bytes from a dictionary key
    pub fn get_bytes(&self, key: &[u8]) -> Result<&'a [u8], Error> {
        match self.get(key) {
            Some(Bencode::Bytes(b)) => Ok(*b),
            _ => Err(Error::InvalidDictKey),
        }
    }

    /// Get integer from a dictionary key
    pub fn get_int(&self, key: &[u8]) -> Result<usize, Error> {
        match self.get(key) {
            Some(Bencode::Int(n)) => Ok(*n as usize),
            _ => Err(Error::InvalidDictKey),
        }
    }

    /// Get list from a dictionary key
    pub fn get_list(&self, key: &[u8]) -> Result<&Vec<Bencode<'a>>, Error> {
        match self.get(key) {
            Some(Bencode::List(l)) => Ok(l),
            _ => Err(Error::InvalidDictKey),
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
        let bencode = super::decode(b"d4:test5:valueee").unwrap();
        let value = bencode.get_bytes(b"test").unwrap();
        assert_eq!(value, b"value");
    }

    #[test]
    fn test_get_bytes_missing_key() {
        let bencode = super::decode(b"d4:test5:valueee").unwrap();
        let err = bencode.get_bytes(b"missing").unwrap_err();
        assert!(matches!(err, super::Error::InvalidDictKey));
    }

    #[test]
    fn test_get_bytes_wrong_type() {
        let bencode = super::decode(b"d4:testi42eee").unwrap();
        let err = bencode.get_bytes(b"test").unwrap_err();
        assert!(matches!(err, super::Error::InvalidDictKey));
    }

    #[test]
    fn test_get_int_valid() {
        let bencode = super::decode(b"d4:testi42eee").unwrap();
        let value = bencode.get_int(b"test").unwrap();
        assert_eq!(value, 42);
    }

    #[test]
    fn test_get_int_missing_key() {
        let bencode = super::decode(b"d4:testi42eee").unwrap();
        let err = bencode.get_int(b"missing").unwrap_err();
        assert!(matches!(err, super::Error::InvalidDictKey));
    }

    #[test]
    fn test_get_int_wrong_type() {
        let bencode = super::decode(b"d4:test5:valueee").unwrap();
        let err = bencode.get_int(b"test").unwrap_err();
        assert!(matches!(err, super::Error::InvalidDictKey));
    }

    #[test]
    fn test_get_list_valid() {
        let bencode = super::decode(b"d4:testl5:valueee").unwrap();
        let list = bencode.get_list(b"test").unwrap();
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn test_get_list_missing_key() {
        let bencode = super::decode(b"d4:testl5:valueee").unwrap();
        let err = bencode.get_list(b"missing").unwrap_err();
        assert!(matches!(err, super::Error::InvalidDictKey));
    }

    #[test]
    fn test_get_list_wrong_type() {
        let bencode = super::decode(b"d4:test5:valueee").unwrap();
        let err = bencode.get_list(b"test").unwrap_err();
        assert!(matches!(err, super::Error::InvalidDictKey));
    }
}
