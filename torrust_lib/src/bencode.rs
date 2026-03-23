#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bencode {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<Bencode>),
    Dict(Vec<(Vec<u8>, Bencode)>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    UnexpectedEof,
    UnexpectedByte { expected: u8, found: u8 },
    InvalidToken(u8),
    InvalidInteger,
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
    pub fn parse(&mut self) -> Result<Bencode, Error> {
        match self.peek_byte()? {
            b'i' => self.parse_int(),
            b'l' => unimplemented!(),
            b'd' => unimplemented!(),
            b'0'..=b'9' => unimplemented!(),
            byte => Err(Error::InvalidToken(byte)),
        }
    }

    fn parse_int(&mut self) -> Result<Bencode, Error> {
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
}
