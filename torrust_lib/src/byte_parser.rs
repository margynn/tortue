#[derive(Debug, PartialEq)]
pub enum Error {
    UnexpectedEOF,
    UnexpectedByte { expected: u8, found: u8 },
}

#[derive(Debug, Clone, Copy)]
pub struct ForwardByteParser<'a>(&'a [u8]);

impl<'a> From<ForwardByteParser<'a>> for &'a [u8] {
    fn from(parser: ForwardByteParser<'a>) -> Self {
        parser.0
    }
}

impl<'a> ForwardByteParser<'a> {
    /// Create a new `ForwardByteParser` instance from a byte slice
    ///
    /// # Example
    /// ```
    /// # use torrust_lib::byte_parser::ForwardByteParser;
    /// let parser = ForwardByteParser::new(&[0x01, 0x02, 0x03]);
    /// assert_eq!(parser.len(), 3);
    /// ```
    #[must_use]
    pub fn new(data: &'a [u8]) -> Self {
        Self(data)
    }

    /// Consume and return u8 from the byte slice
    /// or `UnexpectedEOF` error when the byte slice is empty.
    ///
    /// # Example
    /// ```
    /// # use torrust_lib::byte_parser::ForwardByteParser;
    /// let mut parser = ForwardByteParser::new(&[0x01, 0x02, 0x03]);
    /// assert_eq!(parser.u8().unwrap(), 0x01);
    /// assert_eq!(parser.u8().unwrap(), 0x02);
    /// assert_eq!(parser.u8().unwrap(), 0x03);
    /// ```
    pub fn u8(&mut self) -> Result<u8, Error> {
        let (first, rest) = self.0.split_first().ok_or(Error::UnexpectedEOF)?;
        self.0 = rest;
        Ok(*first)
    }

    /// Return the number of bytes still unparsed
    ///
    /// # Example
    /// ```
    /// # use torrust_lib::byte_parser::ForwardByteParser;
    /// let mut parser = ForwardByteParser::new(&[0x01, 0x02, 0x03]);
    /// assert_eq!(parser.len(), 3);
    /// parser.u8().unwrap();
    /// assert_eq!(parser.len(), 2);
    /// ```
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the byte slice is empty
    ///
    /// # Example
    /// ```
    /// # use torrust_lib::byte_parser::ForwardByteParser;
    /// let parser = ForwardByteParser::new(&[]);
    /// assert!(parser.is_empty());
    /// let parser = ForwardByteParser::new(&[0x01]);
    /// assert!(!parser.is_empty());
    /// ```
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Peek at the next byte without consuming it
    ///
    /// # Example
    /// ```
    /// # use torrust_lib::byte_parser::ForwardByteParser;
    /// let parser = ForwardByteParser::new(&[0x42]);
    /// assert_eq!(parser.peek().unwrap(), 0x42);
    /// assert_eq!(parser.len(), 1);
    /// ```
    pub fn peek(&self) -> Result<u8, Error> {
        let (first, _) = self.0.split_first().ok_or(Error::UnexpectedEOF)?;
        Ok(*first)
    }

    /// Consume the next byte if it matches the expected value
    ///
    /// # Example
    /// ```
    /// # use torrust_lib::byte_parser::ForwardByteParser;
    /// let mut parser = ForwardByteParser::new(&[0x42, 0x43]);
    /// parser.expect(0x42).unwrap();
    /// assert_eq!(parser.len(), 1);
    /// assert_eq!(parser.peek().unwrap(), 0x43);
    /// ```
    pub fn expect(&mut self, expected: u8) -> Result<(), Error> {
        let found = self.peek()?;
        if found == expected {
            self.u8()?;
            Ok(())
        } else {
            Err(Error::UnexpectedByte { expected, found })
        }
    }

    /// Consume and return a slice of `n` bytes
    ///
    /// # Example
    /// ```
    /// # use torrust_lib::byte_parser::ForwardByteParser;
    /// let mut parser = ForwardByteParser::new(&[0x01, 0x02, 0x03]);
    /// assert_eq!(parser.slice(2).unwrap(), &[0x01, 0x02]);
    /// assert_eq!(parser.len(), 1);
    /// ```
    pub fn slice(&mut self, n: usize) -> Result<&'a [u8], Error> {
        if n > self.0.len() {
            return Err(Error::UnexpectedEOF);
        }
        let (slice, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(slice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u8() {
        let mut parser = ForwardByteParser::new(&[0x12, 0x23, 0x34]);
        assert_eq!(parser.0.len(), 3);
        assert_eq!(parser.u8().unwrap(), 0x12);
        assert_eq!(parser.0.len(), 2);
        assert_eq!(parser.u8().unwrap(), 0x23);
        assert_eq!(parser.0.len(), 1);
        assert_eq!(parser.u8().unwrap(), 0x34);
        assert_eq!(parser.0.len(), 0);
        assert_eq!(parser.u8(), Err(Error::UnexpectedEOF));
    }

    #[test]
    fn test_len() {
        let parser = ForwardByteParser::new(&[0x12, 0x23, 0x34]);
        assert_eq!(parser.len(), 3);
        let parser = ForwardByteParser::new(&[0x12]);
        assert_eq!(parser.len(), 1);
        let parser = ForwardByteParser::new(&[]);
        assert_eq!(parser.len(), 0);
    }

    #[test]
    fn test_is_empty() {
        let parser = ForwardByteParser::new(&[0x12, 0x23, 0x34]);
        assert!(!parser.is_empty());
        let parser = ForwardByteParser::new(&[]);
        assert!(parser.is_empty());
    }

    #[test]
    fn test_slice() {
        let mut parser = ForwardByteParser::new(&[0x12, 0x23, 0x34]);
        assert_eq!(&[] as &[u8], parser.slice(0).unwrap());
        assert_eq!(&[0x12, 0x23], parser.slice(2).unwrap());
        assert_eq!(1, parser.0.len());
        assert_eq!(&[0x34], parser.slice(1).unwrap());
        assert_eq!(parser.slice(1), Err(Error::UnexpectedEOF));
        let mut parser = ForwardByteParser::new(&[0x12, 0x23, 0x34]);
        assert_eq!(parser.slice(4), Err(Error::UnexpectedEOF));
        assert_eq!(3, parser.0.len());
        assert_eq!(&[0x12, 0x23, 0x34], parser.slice(3).unwrap());
        assert_eq!(0, parser.0.len());
    }

    #[test]
    fn test_expect() {
        let mut parser = ForwardByteParser::new(&[0x42, 0x43, 0x44]);
        assert_eq!(parser.expect(0x42), Ok(()));
        assert_eq!(parser.len(), 2);
        assert_eq!(
            parser.expect(0x99),
            Err(Error::UnexpectedByte {
                expected: 0x99,
                found: 0x43
            })
        );
        assert_eq!(parser.len(), 2); // not consumed on error
        assert_eq!(parser.expect(0x43), Ok(()));
        assert_eq!(parser.len(), 1);
        assert_eq!(parser.expect(0x44), Ok(()));
        assert_eq!(parser.len(), 0);
        assert_eq!(parser.expect(0x00), Err(Error::UnexpectedEOF));
    }
}
