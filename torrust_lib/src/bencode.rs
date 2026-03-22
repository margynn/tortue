use crate::byte_parser::{self, ForwardByteParser};

#[derive(Debug, PartialEq)]
pub enum Bencode {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<Bencode>),
    Dict(Vec<(Vec<u8>, Bencode)>),
}

#[derive(Debug, PartialEq)]
pub enum Error {
    ByteParser(byte_parser::Error),
    InvalidToken(u8),
    InvalidInteger,
    InvalidStringLength,
}

impl From<byte_parser::Error> for Error {
    fn from(err: byte_parser::Error) -> Self {
        Error::ByteParser(err)
    }
}

pub struct Parser<'a> {
    parser: ForwardByteParser<'a>,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a [u8]) -> Self {
        Parser {
            parser: ForwardByteParser::new(input),
        }
    }

    pub fn parse(&mut self) -> Result<Bencode, Error> {
        let byte = self.parser.peek()?;
        match byte {
            b'i' => self.parse_int(),
            b'l' => unimplemented!(),        //self.parse_list(),
            b'd' => unimplemented!(),        //self.parse_dict(),
            b'0'..=b'9' => unimplemented!(), //self.parse_string(),
            _ => Err(Error::InvalidToken(byte)),
        }
    }

    fn parse_int(&mut self) -> Result<Bencode, Error> {
        self.parser.expect(b'i')?;

        let mut bytes = Vec::new();
        loop {
            let byte = self.parser.u8()?;
            match byte {
                b'e' => break,
                _ => bytes.push(byte),
            }
        }

        let s = std::str::from_utf8(&bytes).map_err(|_| Error::InvalidInteger)?;

        if s.is_empty() {
            return Err(Error::InvalidInteger);
        }
        if s.starts_with('0') && s.len() > 1 {
            return Err(Error::InvalidInteger); // leading zero
        }
        if s.starts_with("-0") {
            return Err(Error::InvalidInteger); // negative zero
        }

        let value: i64 = s.parse().map_err(|_| Error::InvalidInteger)?;

        self.parser.u8()?; // consume 'e'
        Ok(Bencode::Int(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_int() {
        let data = b"i42e";
        let mut parser = Parser::new(data);
        let b = parser.parse_int().unwrap();
        assert_eq!(b, Bencode::Int(42));
    }

    #[test]
    fn test_parse_negative() {
        let data = b"i-10e";
        let mut parser = Parser::new(data);
        let b = parser.parse_int().unwrap();
        assert_eq!(b, Bencode::Int(-10));
    }

    #[test]
    fn test_parse_eof() {
        let data = b"i42";
        let mut parser = Parser::new(data);
        let err = parser.parse_int().unwrap_err();
        assert_eq!(err, Error::ByteParser(byte_parser::Error::UnexpectedEOF));
    }

    #[test]
    fn test_parse_zero() {
        let data = b"i0e";
        let mut parser = Parser::new(data);
        let b = parser.parse_int().unwrap();
        assert_eq!(b, Bencode::Int(0));
    }

    #[test]
    fn test_parse_negative_zero() {
        let data = b"i-0e";
        let mut parser = Parser::new(data);
        let err = parser.parse_int().unwrap_err();
        assert_eq!(err, Error::InvalidInteger);
    }

    #[test]
    fn test_parse_leading_zero() {
        let data = b"i03e";
        let mut parser = Parser::new(data);
        let err = parser.parse_int().unwrap_err();
        assert_eq!(err, Error::InvalidInteger);
    }
}
