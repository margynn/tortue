use std::collections::BTreeMap;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
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

    #[error("invalid string")]
    InvalidString,

    #[error("invalid dictionary key")]
    InvalidDictKey,

    #[error("key not found")]
    KeyNotFound,

    #[error("type mismatch")]
    TypeMismatch,
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bencode<'a> {
    Int(i64),
    Bytes(&'a [u8]),
    List(Vec<Bencode<'a>>),
    Dict(BTreeMap<&'a [u8], Bencode<'a>>),
}

impl<'a> Bencode<'a> {
    pub fn decode(data: &'a [u8]) -> Result<Self> {
        Decoder::new(data).parse()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode_into(&mut buf);
        buf
    }

    fn encode_into(&self, buf: &mut Vec<u8>) {
        match self {
            Bencode::Int(n) => {
                buf.push(b'i');
                buf.extend_from_slice(n.to_string().as_bytes());
                buf.push(b'e');
            }
            Bencode::Bytes(bytes) => {
                buf.extend_from_slice(bytes.len().to_string().as_bytes());
                buf.push(b':');
                buf.extend_from_slice(bytes);
            }
            Bencode::List(items) => {
                buf.push(b'l');
                for item in items {
                    item.encode_into(buf);
                }
                buf.push(b'e');
            }
            Bencode::Dict(entries) => {
                buf.push(b'd');
                for (key, value) in entries {
                    buf.extend_from_slice(key.len().to_string().as_bytes());
                    buf.push(b':');
                    buf.extend_from_slice(key);
                    value.encode_into(buf);
                }
                buf.push(b'e');
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<&Bencode<'a>> {
        match self {
            Bencode::Dict(entries) => entries.get(key).ok_or(Error::KeyNotFound),
            _ => Err(Error::TypeMismatch),
        }
    }

    pub fn get_utf8(&self, key: &[u8]) -> Result<String> {
        let bytes = self.get_bytes(key)?;
        std::str::from_utf8(bytes).map(|s| s.to_owned()).map_err(|_| Error::InvalidString)
    }

    pub fn get_bytes(&self, key: &[u8]) -> Result<&'a [u8]> {
        match self.get(key)? {
            Bencode::Bytes(bytes) => Ok(bytes),
            _ => Err(Error::TypeMismatch),
        }
    }

    pub fn get_int(&self, key: &[u8]) -> Result<i64> {
        match self.get(key)? {
            Bencode::Int(n) => Ok(*n),
            _ => Err(Error::TypeMismatch),
        }
    }

    pub fn get_list(&self, key: &[u8]) -> Result<&[Bencode<'a>]> {
        match self.get(key)? {
            Bencode::List(list) => Ok(list.as_slice()),
            _ => Err(Error::TypeMismatch),
        }
    }
}

struct Decoder<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, position: 0 }
    }

    fn parse(&mut self) -> Result<Bencode<'a>> {
        match self.peek_byte()? {
            b'i' => self.parse_int(),
            b'l' => self.parse_list(),
            b'd' => self.parse_dict(),
            b'0'..=b'9' => self.parse_bytes(),
            byte => Err(Error::InvalidToken(byte)),
        }
    }

    fn remaining(&self) -> usize {
        self.input.len().saturating_sub(self.position)
    }

    fn peek_byte(&self) -> Result<u8> {
        self.input.get(self.position).copied().ok_or(Error::UnexpectedEof)
    }

    fn next_byte(&mut self) -> Result<u8> {
        let byte = self.peek_byte()?;
        self.position += 1;
        Ok(byte)
    }

    fn consume_byte(&mut self, expected: u8) -> Result<()> {
        let found = self.next_byte()?;
        if found != expected {
            return Err(Error::UnexpectedByte { expected, found });
        }
        Ok(())
    }

    fn parse_int(&mut self) -> Result<Bencode<'a>> {
        self.consume_byte(b'i')?;
        let start = self.position;
        while self.peek_byte()? != b'e' {
            self.position += 1;
        }
        let s = std::str::from_utf8(&self.input[start..self.position])
            .map_err(|_| Error::InvalidInteger)?;
        if s.is_empty() || s.starts_with("-0") || (s.starts_with('0') && s.len() > 1) {
            return Err(Error::InvalidInteger);
        }
        let value = s.parse::<i64>().map_err(|_| Error::InvalidInteger)?;
        self.consume_byte(b'e')?;
        Ok(Bencode::Int(value))
    }

    fn parse_bytes(&mut self) -> Result<Bencode<'a>> {
        Ok(Bencode::Bytes(self.parse_byte_string()?))
    }

    fn parse_byte_string(&mut self) -> Result<&'a [u8]> {
        let len = self.parse_len()?;
        if len > self.remaining() {
            return Err(Error::UnexpectedEof);
        }
        let start = self.position;
        self.position += len;
        Ok(&self.input[start..self.position])
    }

    fn parse_len(&mut self) -> Result<usize> {
        let start = self.position;
        loop {
            match self.peek_byte()? {
                b':' => break,
                b'0'..=b'9' => self.position += 1,
                _ => return Err(Error::InvalidStringLength),
            }
        }
        let len = std::str::from_utf8(&self.input[start..self.position])
            .map_err(|_| Error::InvalidStringLength)?
            .parse::<usize>()
            .map_err(|_| Error::InvalidStringLength)?;
        self.consume_byte(b':')?;
        Ok(len)
    }

    fn parse_list(&mut self) -> Result<Bencode<'a>> {
        self.consume_byte(b'l')?;
        let mut elements = Vec::new();
        while self.peek_byte()? != b'e' {
            elements.push(self.parse()?);
        }
        self.consume_byte(b'e')?;
        Ok(Bencode::List(elements))
    }

    fn parse_dict(&mut self) -> Result<Bencode<'a>> {
        self.consume_byte(b'd')?;
        let mut dict = BTreeMap::new();
        while self.peek_byte()? != b'e' {
            let key = self.parse_byte_string().map_err(|_| Error::InvalidDictKey)?;
            let value = self.parse()?;
            dict.insert(key, value);
        }
        self.consume_byte(b'e')?;
        Ok(Bencode::Dict(dict))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn decode_integers() {
        assert_eq!(Bencode::decode(b"i42e"), Ok(Bencode::Int(42)));
        assert_eq!(Bencode::decode(b"i-10e"), Ok(Bencode::Int(-10)));
        assert_eq!(Bencode::decode(b"i0e"), Ok(Bencode::Int(0)));
        assert_eq!(Bencode::decode(b"i42"), Err(Error::UnexpectedEof));
        assert_eq!(Bencode::decode(b"i-0e"), Err(Error::InvalidInteger));
        assert_eq!(Bencode::decode(b"i03e"), Err(Error::InvalidInteger));
    }

    #[test]
    fn decode_bytes() {
        assert_eq!(Bencode::decode(b"4:spam"), Ok(Bencode::Bytes(b"spam")));
        assert_eq!(Bencode::decode(b"0:"), Ok(Bencode::Bytes(b"")));
        assert_eq!(Bencode::decode(b"4:spa"), Err(Error::UnexpectedEof));
        assert_eq!(Bencode::decode(b"4spam"), Err(Error::InvalidStringLength));
        assert_eq!(Bencode::decode(b"4a:spam"), Err(Error::InvalidStringLength));
        assert_eq!(Bencode::decode(b"10:"), Err(Error::UnexpectedEof));
    }

    #[test]
    fn decode_list() {
        assert_eq!(Bencode::decode(b"le"), Ok(Bencode::List(vec![])));
        assert_eq!(
            Bencode::decode(b"li1ei2ei3ee"),
            Ok(Bencode::List(vec![Bencode::Int(1), Bencode::Int(2), Bencode::Int(3)]))
        );
        assert_eq!(
            Bencode::decode(b"lli1ei2eei3ee"),
            Ok(Bencode::List(vec![
                Bencode::List(vec![Bencode::Int(1), Bencode::Int(2)]),
                Bencode::Int(3),
            ]))
        );
        assert_eq!(Bencode::decode(b"li1ei2e"), Err(Error::UnexpectedEof));
        assert_eq!(Bencode::decode(b"lxe"), Err(Error::InvalidToken(b'x')));
    }

    #[test]
    fn decode_dict() {
        assert_eq!(Bencode::decode(b"de"), Ok(Bencode::Dict(BTreeMap::new())));
        assert_eq!(
            Bencode::decode(b"d3:cow3:mooe"),
            Ok(Bencode::Dict(BTreeMap::from([(b"cow".as_slice(), Bencode::Bytes(b"moo"))])))
        );
        assert_eq!(
            Bencode::decode(b"d3:food3:bari1eee"),
            Ok(Bencode::Dict(BTreeMap::from([(
                b"foo".as_slice(),
                Bencode::Dict(BTreeMap::from([(b"bar".as_slice(), Bencode::Int(1))])),
            )])))
        );
        assert_eq!(Bencode::decode(b"d3:cow3:moo"), Err(Error::UnexpectedEof));
        assert_eq!(Bencode::decode(b"d3:cowe"), Err(Error::InvalidToken(b'e')));
        assert_eq!(Bencode::decode(b"di1e3:mooe"), Err(Error::InvalidDictKey));
        assert_eq!(Bencode::decode(b"d3a:foo3:bare"), Err(Error::InvalidDictKey));
    }

    #[test]
    fn encode_values() {
        assert_eq!(Bencode::Int(42).encode(), b"i42e");
        assert_eq!(Bencode::Int(-10).encode(), b"i-10e");
        assert_eq!(Bencode::Bytes(b"hello").encode(), b"5:hello");
        assert_eq!(Bencode::Bytes(b"").encode(), b"0:");
        assert_eq!(
            Bencode::List(vec![Bencode::Int(1), Bencode::Bytes(b"test"), Bencode::List(vec![])]).encode(),
            b"li1e4:testlee"
        );
        assert_eq!(
            Bencode::Dict(BTreeMap::from([
                (b"age".as_slice(), Bencode::Int(30)),
                (b"name".as_slice(), Bencode::Bytes(b"John")),
            ])).encode(),
            b"d3:agei30e4:name4:Johne"
        );
    }

    #[test]
    fn roundtrip() {
        for encoded in [b"i42e".as_slice(), b"4:spam", b"li1e4:spame", b"d3:fooi1ee"] {
            assert_eq!(Bencode::decode(encoded).unwrap().encode(), encoded);
        }
    }

    #[test]
    fn accessors() {
        let data = b"d3:agei30e4:name4:John4:tagsl3:devee";
        let b = Bencode::decode(data).unwrap();

        assert_eq!(b.get_int(b"age"), Ok(30));
        assert_eq!(b.get_utf8(b"name"), Ok("John".to_owned()));
        assert_eq!(b.get_bytes(b"name"), Ok(b"John".as_slice()));
        assert_eq!(b.get_list(b"tags").unwrap().len(), 1);

        assert_eq!(b.get(b"missing"), Err(Error::KeyNotFound));
        assert_eq!(b.get_int(b"name"), Err(Error::TypeMismatch));
        assert_eq!(b.get_bytes(b"age"), Err(Error::TypeMismatch));
        assert_eq!(Bencode::Int(0).get(b"x"), Err(Error::TypeMismatch));
    }
}
