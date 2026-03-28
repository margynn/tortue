use super::{Bencode, Error};

pub(super) fn decode(data: &[u8]) -> Result<Bencode<'_>, Error> {
    Decoder::new(data).parse()
}

struct Decoder<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, position: 0 }
    }

    fn parse(&mut self) -> Result<Bencode<'a>, Error> {
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

    fn peek_byte(&self) -> Result<u8, Error> {
        self.input.get(self.position).copied().ok_or(Error::UnexpectedEof)
    }

    fn next_byte(&mut self) -> Result<u8, Error> {
        let byte: u8 = self.peek_byte()?;
        self.position += 1;
        Ok(byte)
    }

    fn consume_byte(&mut self, expected: u8) -> Result<(), Error> {
        let found: u8 = self.next_byte()?;
        if found != expected {
            return Err(Error::UnexpectedByte { expected, found });
        }
        Ok(())
    }

    fn parse_int(&mut self) -> Result<Bencode<'a>, Error> {
        self.consume_byte(b'i')?;

        let start: usize = self.position;
        while self.peek_byte()? != b'e' {
            self.position += 1;
        }

        let s: &str = std::str::from_utf8(&self.input[start..self.position])
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

        let value: i64 = s.parse::<i64>().map_err(|_| Error::InvalidInteger)?;
        self.consume_byte(b'e')?;

        Ok(Bencode::Int(value))
    }

    fn parse_bytes(&mut self) -> Result<Bencode<'a>, Error> {
        Ok(Bencode::Bytes(self.parse_byte_string()?))
    }

    fn parse_byte_string(&mut self) -> Result<&'a [u8], Error> {
        let len: usize = self.parse_len()?;
        if len > self.remaining() {
            return Err(Error::UnexpectedEof);
        }
        let start: usize = self.position;
        let end: usize = start + len;
        self.position = end;
        Ok(&self.input[start..end])
    }

    fn parse_len(&mut self) -> Result<usize, Error> {
        let start: usize = self.position;
        while self.peek_byte()? != b':' {
            match self.peek_byte()? {
                b'0'..=b'9' => self.position += 1,
                _ => return Err(Error::InvalidStringLength),
            }
        }
        let len: usize = std::str::from_utf8(&self.input[start..self.position])
            .map_err(|_| Error::InvalidStringLength)?
            .parse::<usize>()
            .map_err(|_| Error::InvalidStringLength)?;

        self.consume_byte(b':')?;
        Ok(len)
    }

    fn parse_list(&mut self) -> Result<Bencode<'a>, Error> {
        self.consume_byte(b'l')?;
        let mut elements = Vec::new();
        while self.peek_byte()? != b'e' {
            elements.push(self.parse()?);
        }
        self.consume_byte(b'e')?;
        Ok(Bencode::List(elements))
    }

    fn parse_dict(&mut self) -> Result<Bencode<'a>, Error> {
        self.consume_byte(b'd')?;
        let mut dict = Vec::new();
        while self.peek_byte()? != b'e' {
            let key =
                self.parse_byte_string().map_err(|_| Error::InvalidDictKey)?;
            let value = self.parse()?;
            dict.push((key, value));
        }
        self.consume_byte(b'e')?;
        Ok(Bencode::Dict(dict))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_int() {
        let data = b"i42e";
        let mut decoder = Decoder::new(data);
        let b = decoder.parse_int().unwrap();
        assert_eq!(b, Bencode::Int(42));
    }

    #[test]
    fn test_parse_ints() {
        let data = b"i42ei-42e";
        let mut decoder = Decoder::new(data);
        let b = decoder.parse_int().unwrap();
        assert_eq!(b, Bencode::Int(42));
        let b = decoder.parse_int().unwrap();
        assert_eq!(b, Bencode::Int(-42));
    }

    #[test]
    fn test_parse_negative() {
        let data = b"i-10e";
        let mut decoder = Decoder::new(data);
        let b = decoder.parse_int().unwrap();
        assert_eq!(b, Bencode::Int(-10));
    }

    #[test]
    fn test_parse_eof() {
        let data = b"i42";
        let mut decoder = Decoder::new(data);
        let err = decoder.parse_int().unwrap_err();
        assert_eq!(err, Error::UnexpectedEof);
    }

    #[test]
    fn test_parse_zero() {
        let data = b"i0e";
        let mut decoder = Decoder::new(data);
        let b = decoder.parse_int().unwrap();
        assert_eq!(b, Bencode::Int(0));
    }

    #[test]
    fn test_parse_negative_zero() {
        let data = b"i-0e";
        let mut decoder = Decoder::new(data);
        let err = decoder.parse_int().unwrap_err();
        assert_eq!(err, Error::InvalidInteger);
    }

    #[test]
    fn test_parse_leading_zero() {
        let data = b"i03e";
        let mut decoder = Decoder::new(data);
        let err = decoder.parse_int().unwrap_err();
        assert_eq!(err, Error::InvalidInteger);
    }

    #[test]
    fn test_parse_bytes() {
        let data = b"4:spam";
        let mut decoder = Decoder::new(data);
        let b = decoder.parse_bytes().unwrap();
        assert_eq!(b, Bencode::Bytes(b"spam"));
    }

    #[test]
    fn test_parse_empty_bytes() {
        let data = b"0:";
        let mut decoder = Decoder::new(data);
        let b = decoder.parse_bytes().unwrap();
        assert_eq!(b, Bencode::Bytes(b""));
    }

    #[test]
    fn test_parse_bytes_sequentially() {
        let data = b"4:spam3:egg";
        let mut decoder = Decoder::new(data);
        let first = decoder.parse_bytes().unwrap();
        assert_eq!(first, Bencode::Bytes(b"spam"));
        let second = decoder.parse_bytes().unwrap();
        assert_eq!(second, Bencode::Bytes(b"egg"));
    }

    #[test]
    fn test_parse_bytes_truncated_payload() {
        let data = b"4:spa";
        let mut decoder = Decoder::new(data);
        let err = decoder.parse_bytes().unwrap_err();
        assert_eq!(err, Error::UnexpectedEof);
    }

    #[test]
    fn test_parse_bytes_missing_colon() {
        let data = b"4spam";
        let mut decoder = Decoder::new(data);
        let err = decoder.parse_bytes().unwrap_err();
        assert_eq!(err, Error::InvalidStringLength);
    }

    #[test]
    fn test_parse_bytes_invalid_length_char() {
        let data = b"4a:spam";
        let mut decoder = Decoder::new(data);
        let err = decoder.parse_bytes().unwrap_err();
        assert_eq!(err, Error::InvalidStringLength);
    }

    #[test]
    fn test_parse_bytes_large_length_with_no_payload() {
        let data = b"10:";
        let mut decoder = Decoder::new(data);
        let err = decoder.parse_bytes().unwrap_err();
        assert_eq!(err, Error::UnexpectedEof);
    }

    #[test]
    fn test_parse_empty_list() {
        let data = b"le";
        let mut decoder = Decoder::new(data);
        let v = decoder.parse().unwrap();
        assert_eq!(v, Bencode::List(vec![]));
    }

    #[test]
    fn test_parse_list_of_ints() {
        let data = b"li1ei2ei3ee";
        let mut decoder = Decoder::new(data);
        let v = decoder.parse().unwrap();
        assert_eq!(
            v,
            Bencode::List(vec![
                Bencode::Int(1),
                Bencode::Int(2),
                Bencode::Int(3),
            ])
        );
    }

    #[test]
    fn test_parse_list_of_bytes() {
        let data = b"l4:spam4:eggs3:eyee";
        let mut decoder = Decoder::new(data);
        let v = decoder.parse().unwrap();
        assert_eq!(
            v,
            Bencode::List(vec![
                Bencode::Bytes(b"spam"),
                Bencode::Bytes(b"eggs"),
                Bencode::Bytes(b"eye")
            ])
        );
    }

    #[test]
    fn test_parse_nested_list() {
        let data = b"lli1ei2eei3ee";
        let mut decoder = Decoder::new(data);
        let v = decoder.parse().unwrap();
        assert_eq!(
            v,
            Bencode::List(vec![
                Bencode::List(vec![Bencode::Int(1), Bencode::Int(2),]),
                Bencode::Int(3),
            ])
        );
    }

    #[test]
    fn test_parse_mixed_list() {
        let data = b"li1e4:spaml3:abceee";
        let mut decoder = Decoder::new(data);
        let v = decoder.parse().unwrap();
        assert_eq!(
            v,
            Bencode::List(vec![
                Bencode::Int(1),
                Bencode::Bytes(b"spam"),
                Bencode::List(vec![Bencode::Bytes(b"abc"),]),
            ])
        );
    }

    #[test]
    fn test_parse_list_missing_end() {
        let data = b"li1ei2e";
        let mut decoder = Decoder::new(data);
        let err = decoder.parse().unwrap_err();
        assert_eq!(err, Error::UnexpectedEof);
    }

    #[test]
    fn test_parse_list_invalid_token_inside() {
        let data = b"lxe";
        let mut decoder = Decoder::new(data);
        let err = decoder.parse().unwrap_err();
        assert_eq!(err, Error::InvalidToken(b'x'));
    }

    #[test]
    fn test_parse_list_sequential() {
        let data = b"li1eel3:abce";
        let mut decoder = Decoder::new(data);
        let v1 = decoder.parse().unwrap();
        assert_eq!(v1, Bencode::List(vec![Bencode::Int(1)]));
        let v2 = decoder.parse().unwrap();
        assert_eq!(v2, Bencode::List(vec![Bencode::Bytes(b"abc")]));
    }

    #[test]
    fn test_parse_empty_dict() {
        let data = b"de";
        let mut decoder = Decoder::new(data);
        let v = decoder.parse().unwrap();
        assert_eq!(v, Bencode::Dict(vec![]));
    }

    #[test]
    fn test_parse_dict_single_pair() {
        let data = b"d3:cow3:mooe";
        let mut decoder = Decoder::new(data);
        let v = decoder.parse().unwrap();
        assert_eq!(
            v,
            Bencode::Dict(vec![(b"cow".as_slice(), Bencode::Bytes(b"moo")),])
        );
    }

    #[test]
    fn test_parse_dict_multiple_pairs() {
        let data = b"d3:cow3:moo4:spam4:eggse";
        let mut decoder = Decoder::new(data);
        let v = decoder.parse().unwrap();
        assert_eq!(
            v,
            Bencode::Dict(vec![
                (b"cow".as_slice(), Bencode::Bytes(b"moo")),
                (b"spam".as_slice(), Bencode::Bytes(b"eggs")),
            ])
        );
    }

    #[test]
    fn test_parse_dict_with_int_values() {
        let data = b"d3:fooi42e3:bari-7ee";
        let mut decoder = Decoder::new(data);
        let v = decoder.parse().unwrap();
        assert_eq!(
            v,
            Bencode::Dict(vec![
                (b"foo".as_slice(), Bencode::Int(42)),
                (b"bar".as_slice(), Bencode::Int(-7)),
            ])
        );
    }

    #[test]
    fn test_parse_dict_with_list_value() {
        let data = b"d4:listli1ei2ei3eee";
        let mut decoder = Decoder::new(data);
        let v = decoder.parse().unwrap();
        assert_eq!(
            v,
            Bencode::Dict(vec![(
                b"list".as_slice(),
                Bencode::List(vec![
                    Bencode::Int(1),
                    Bencode::Int(2),
                    Bencode::Int(3),
                ]),
            ),])
        );
    }

    #[test]
    fn test_parse_nested_dict() {
        let data = b"d3:food3:bari1eee";
        let mut decoder = Decoder::new(data);
        let v = decoder.parse().unwrap();
        assert_eq!(
            v,
            Bencode::Dict(vec![(
                b"foo".as_slice(),
                Bencode::Dict(vec![(b"bar".as_slice(), Bencode::Int(1)),]),
            ),])
        );
    }

    #[test]
    fn test_parse_dict_sequentially() {
        let data = b"d3:foo3:bared3:bazi1ee";
        let mut decoder = Decoder::new(data);
        let first = decoder.parse().unwrap();
        assert_eq!(
            first,
            Bencode::Dict(vec![(b"foo".as_slice(), Bencode::Bytes(b"bar")),])
        );

        let second = decoder.parse().unwrap();
        assert_eq!(
            second,
            Bencode::Dict(vec![(b"baz".as_slice(), Bencode::Int(1)),])
        );
    }

    #[test]
    fn test_parse_dict_missing_end() {
        let data = b"d3:cow3:moo";
        let mut decoder = Decoder::new(data);
        let err = decoder.parse().unwrap_err();
        assert_eq!(err, Error::UnexpectedEof);
    }

    #[test]
    fn test_parse_dict_missing_value() {
        let data = b"d3:cowe";
        let mut decoder = Decoder::new(data);
        let err = decoder.parse().unwrap_err();
        assert_eq!(err, Error::InvalidToken(b'e'));
    }

    #[test]
    fn test_parse_dict_invalid_key() {
        let data = b"di1e3:mooe";
        let mut decoder = Decoder::new(data);
        let err = decoder.parse().unwrap_err();
        assert_eq!(err, Error::InvalidDictKey);
    }

    #[test]
    fn test_parse_dict_invalid_key_length() {
        let data = b"d3a:foo3:bare";
        let mut decoder = Decoder::new(data);
        let err = decoder.parse_dict().unwrap_err();
        assert_eq!(err, Error::InvalidDictKey);
    }
}
