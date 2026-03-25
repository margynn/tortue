use crate::bencode::{Bencode, Error};

/// Encoder for Bencode data structures
pub struct Encoder<'a> {
    output: Vec<u8>,
}

impl<'a> Encoder<'a> {
    pub(super) fn new() -> Self {
        Self { output: Vec::new(), position: 0 }
    }

    fn remaining(&self) -> usize {
        self.output.len().saturating_sub(self.position)
    }

    fn push_byte(&mut self, byte: u8) {
        self.output.push(byte);
    }

    fn push_bytes(&mut self, bytes: &[u8]) {
        self.output.extend_from_slice(bytes);
    }

    fn encode_int(&mut self, value: i64) -> Result<(), Error> {
        let s = format!("{}", value);
        self.push_byte(b'i');
        self.push_bytes(s.as_bytes());
        self.push_byte(b'e');
        Ok(())
    }

    fn encode_bytes(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let len = bytes.len();
        self.encode_int(len as i64)?;
        self.push_byte(b':');
        self.push_bytes(bytes);
        Ok(())
    }

    fn encode_list(&mut self, items: Vec<Bencode<'a>>) -> Result<(), Error> {
        self.push_byte(b'l');
        for item in items {
            self.encode(item)?;
        }
        self.push_byte(b'e');
        Ok(())
    }

    fn encode_dict(&mut self, entries: Vec<(&'a [u8], Bencode<'a>)>) -> Result<(), Error> {
        self.push_byte(b'd');
        for (key, value) in entries {
            self.encode_bytes(key)?;
            self.encode(value)?;
        }
        self.push_byte(b'e');
        Ok(())
    }

    pub(super) fn encode(&mut self, item: Bencode<'a>) -> Result<(), Error> {
        match item {
            Bencode::Int(v) => self.encode_int(v),
            Bencode::Bytes(b) => self.encode_bytes(b),
            Bencode::List(l) => self.encode_list(l),
            Bencode::Dict(d) => self.encode_dict(d),
        }
    }

    pub(super) fn finish(self) -> Vec<u8> {
        self.output
    }
}
