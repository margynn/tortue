#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bencode<'a> {
    Int(i64),
    Bytes(&'a [u8]),
    List(Vec<Bencode<'a>>),
    Dict(Vec<(&'a [u8], Bencode<'a>)>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    UnexpectedEof,
    UnexpectedByte { expected: u8, found: u8 },
    InvalidToken(u8),
    InvalidInteger,
    InvalidStringLength,
}

pub struct Parser<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a [u8]) -> Self {
        Self { input, position: 0 }
    }

    /// Returns the number of bytes remaining to parse.
    ///
    /// # Examples
    /// ```
    /// # use torrust_lib::bencode::Parser;
    /// let mut parser = Parser::new(b"abc");
    /// assert_eq!(parser.remaining(), 3);
    /// parser.next_byte().unwrap();
    /// assert_eq!(parser.remaining(), 2);
    /// ```
    pub fn remaining(&self) -> usize {
        self.input.len().saturating_sub(self.position)
    }

    /// Returns true when the parser has consumed the full input.
    ///
    /// # Examples
    /// ```
    /// # use torrust_lib::bencode::Parser;
    /// let mut parser = Parser::new(b"a");
    /// assert!(!parser.is_eof());
    /// parser.next_byte().unwrap();
    /// assert!(parser.is_eof());
    /// ```
    pub fn is_eof(&self) -> bool {
        self.position >= self.input.len()
    }

    /// Peeks the next byte without consuming it.
    ///
    /// # Examples
    /// ```
    /// # use torrust_lib::bencode::Parser;
    /// let parser = Parser::new(b"abc");
    /// assert_eq!(parser.peek_byte().unwrap(), b'a');
    /// assert_eq!(parser.peek_byte().unwrap(), b'a');
    /// ```
    pub fn peek_byte(&self) -> Result<u8, Error> {
        self.input
            .get(self.position)
            .copied()
            .ok_or(Error::UnexpectedEof)
    }

    /// Consumes and returns the next byte.
    ///
    /// # Examples
    /// ```
    /// # use torrust_lib::bencode::Parser;
    /// let mut parser = Parser::new(b"ab");
    /// assert_eq!(parser.next_byte().unwrap(), b'a');
    /// assert_eq!(parser.next_byte().unwrap(), b'b');
    /// assert_eq!(parser.next_byte().unwrap_err(), torrust_lib::bencode::Error::UnexpectedEof);
    /// ```
    #[must_use]
    pub fn next_byte(&mut self) -> Result<u8, Error> {
        let byte = self.peek_byte()?;
        self.position += 1;
        Ok(byte)
    }

    /// Consumes the next byte if it matches `expected`.
    ///
    /// # Examples
    /// ```
    /// # use torrust_lib::bencode::{Error, Parser};
    /// let mut parser = Parser::new(b"xy");
    /// parser.consume_byte(b'x').unwrap();
    /// assert_eq!(parser.peek_byte().unwrap(), b'y');
    ///
    /// let mut parser = Parser::new(b"xy");
    /// assert_eq!(
    ///     parser.consume_byte(b'z').unwrap_err(),
    ///     Error::UnexpectedByte { expected: b'z', found: b'x' }
    /// );
    /// ```
    pub fn consume_byte(&mut self, expected: u8) -> Result<(), Error> {
        let found = self.next_byte()?;
        if found != expected {
            return Err(Error::UnexpectedByte { expected, found });
        }
        Ok(())
    }

    #[must_use]
    pub fn parse(&mut self) -> Result<Bencode<'a>, Error> {
        match self.peek_byte()? {
            b'i' => self.parse_int(),
            b'l' => unimplemented!(),
            b'd' => unimplemented!(),
            b'0'..=b'9' => self.parse_bytes(),
            byte => Err(Error::InvalidToken(byte)),
        }
    }

    fn parse_int(&mut self) -> Result<Bencode<'a>, Error> {
        self.consume_byte(b'i')?;

        let start = self.position;
        while self.peek_byte()? != b'e' {
            self.position += 1;
        }

        let s = std::str::from_utf8(&self.input[start..self.position])
            .map_err(|_| Error::InvalidInteger)?;

        if s.is_empty() {
            return Err(Error::InvalidInteger);
        }
        if s.starts_with("-0") {
            return Err(Error::InvalidInteger);
        }
        if s.starts_with('0') && s.len() > 1 {
            return Err(Error::InvalidInteger);
        }

        let value = s.parse::<i64>().map_err(|_| Error::InvalidInteger)?;
        self.consume_byte(b'e')?;

        Ok(Bencode::Int(value))
    }

    fn parse_bytes(&mut self) -> Result<Bencode<'a>, Error> {
        let start = self.position;
        while self.peek_byte()? != b':' {
            match self.peek_byte()? {
                b'0'..=b'9' => self.position += 1,
                _ => return Err(Error::InvalidStringLength),
            }
        }

        let len = std::str::from_utf8(&self.input[start..self.position])
            .map_err(|_| Error::InvalidStringLength)?
            .parse::<usize>()
            .map_err(|_| Error::InvalidStringLength)?;

        self.consume_byte(b':')?;

        if len > self.remaining() {
            return Err(Error::UnexpectedEof);
        }

        let start = self.position;
        let end = start + len;
        self.position = end;

        Ok(Bencode::Bytes(&self.input[start..end]))
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
    fn test_parse_ints() {
        let data = b"i42ei-42e";
        let mut parser = Parser::new(data);
        let b = parser.parse_int().unwrap();
        assert_eq!(b, Bencode::Int(42));
        let b = parser.parse_int().unwrap();
        assert_eq!(b, Bencode::Int(-42));
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
        assert_eq!(err, Error::UnexpectedEof);
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

    #[test]
    fn test_parse_bytes() {
        let data = b"4:spam";
        let mut parser = Parser::new(data);
        let b = parser.parse_bytes().unwrap();
        assert_eq!(b, Bencode::Bytes(b"spam"));
        assert!(parser.is_eof());
    }

    #[test]
    fn test_parse_empty_bytes() {
        let data = b"0:";
        let mut parser = Parser::new(data);
        let b = parser.parse_bytes().unwrap();
        assert_eq!(b, Bencode::Bytes(b""));
        assert!(parser.is_eof());
    }

    #[test]
    fn test_parse_bytes_sequentially() {
        let data = b"4:spam3:egg";
        let mut parser = Parser::new(data);

        let first = parser.parse_bytes().unwrap();
        assert_eq!(first, Bencode::Bytes(b"spam"));

        let second = parser.parse_bytes().unwrap();
        assert_eq!(second, Bencode::Bytes(b"egg"));

        assert!(parser.is_eof());
    }

    #[test]
    fn test_parse_bytes_truncated_payload() {
        let data = b"4:spa";
        let mut parser = Parser::new(data);
        let err = parser.parse_bytes().unwrap_err();
        assert_eq!(err, Error::UnexpectedEof);
    }

    #[test]
    fn test_parse_bytes_missing_colon() {
        let data = b"4spam";
        let mut parser = Parser::new(data);
        let err = parser.parse_bytes().unwrap_err();
        assert_eq!(err, Error::InvalidStringLength);
    }

    #[test]
    fn test_parse_bytes_invalid_length_char() {
        let data = b"4a:spam";
        let mut parser = Parser::new(data);
        let err = parser.parse_bytes().unwrap_err();
        assert_eq!(err, Error::InvalidStringLength);
    }

    #[test]
    fn test_parse_bytes_large_length_with_no_payload() {
        let data = b"10:";
        let mut parser = Parser::new(data);
        let err = parser.parse_bytes().unwrap_err();
        assert_eq!(err, Error::UnexpectedEof);
    }
}
