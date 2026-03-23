mod decoder;

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
    // #[must_use]
    // pub fn encode(&self) -> Vec<u8> {
    //     unimplemented!("encoding not yet implemented")
    // }

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
}
