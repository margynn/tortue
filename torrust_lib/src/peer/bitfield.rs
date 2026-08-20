use super::Error;

const BITS_PER_BYTE: usize = 8;

// MSB-first bitfield.
#[derive(Debug)]
pub struct Bitfield {
    data: Vec<u8>,
    pieces: usize,
}

impl Bitfield {
    pub fn new(pieces: usize) -> Self {
        let data = vec![0; pieces.div_ceil(BITS_PER_BYTE)];
        Self { data, pieces }
    }

    pub fn set_bit(&mut self, bit: usize) -> Result<(), Error> {
        self.check_bit(bit)?;
        let index = bit / BITS_PER_BYTE;
        let offset = bit % BITS_PER_BYTE;
        let mask = 1u8 << (7 - offset);
        self.data[index] |= mask;
        Ok(())
    }

    pub fn extend_bytes(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if bytes.len() != self.data.len() {
            return Err(Error::PieceOutOfRange);
        }
        for (i, v) in bytes.iter().enumerate() {
            self.data[i] |= v;
        }
        Ok(())
    }

    pub fn has_bit(&self, bit: usize) -> Result<bool, Error> {
        self.check_bit(bit)?;
        let index = bit / BITS_PER_BYTE;
        let offset = bit % BITS_PER_BYTE;
        let mask = 1u8 << (7 - offset);
        Ok((self.data[index] & mask) != 0)
    }

    fn check_bit(&self, bit: usize) -> Result<(), Error> {
        if bit >= self.pieces {
            return Err(Error::PieceOutOfRange);
        }
        Ok(())
    }

    pub fn completion_ratio(&self) -> f32 {
        let mut set_bits = 0usize;
        for byte in &self.data {
            set_bits += byte.count_ones() as usize;
        }
        // Important: last byte may contain extra unused bits
        let valid_bits = self.pieces;
        (set_bits.min(valid_bits) as f32) / (valid_bits as f32)
    }
}

impl TryFrom<&[u8]> for Bitfield {
    type Error = &'static str;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let mut bf = Self::new(bytes.len() * BITS_PER_BYTE);
        bf.extend_bytes(bytes).map_err(|_| "failed to extend bytes")?;
        Ok(bf)
    }
}

impl<'a> IntoIterator for &'a Bitfield {
    type Item = u32;
    type IntoIter = BitfieldIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        BitfieldIter {
            bitfield: self.data.as_ref(),
            byte_idx: 0,
            bit_idx: 0,
        }
    }
}

pub struct BitfieldIter<'a> {
    bitfield: &'a [u8],
    byte_idx: usize,
    bit_idx: u8,
}

impl<'a> Iterator for BitfieldIter<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        while self.byte_idx < self.bitfield.len() {
            let byte = self.bitfield[self.byte_idx];

            while self.bit_idx < 8 {
                if (byte >> (7 - self.bit_idx)) & 1 == 1 {
                    let idx = self.byte_idx * 8 + self.bit_idx as usize;
                    self.bit_idx += 1;
                    return Some(idx as u32);
                }
                self.bit_idx += 1;
            }

            self.byte_idx += 1;
            self.bit_idx = 0;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_allocates_zeroed_storage() {
        let bf = Bitfield::new(0);
        assert_eq!(bf.data.len(), 0);

        let bf = Bitfield::new(1);
        assert_eq!(bf.data, vec![0]);

        let bf = Bitfield::new(8);
        assert_eq!(bf.data, vec![0]);

        let bf = Bitfield::new(9);
        assert_eq!(bf.data, vec![0, 0]);

        let bf = Bitfield::new(16);
        assert_eq!(bf.data, vec![0, 0]);

        let bf = Bitfield::new(17);
        assert_eq!(bf.data, vec![0, 0, 0]);
    }

    #[test]
    fn has_bit_is_false_for_new_bitfield() {
        let bf = Bitfield::new(8);

        for bit in 0..8 {
            assert!(!bf.has_bit(bit).unwrap());
        }
    }

    #[test]
    fn set_bit_sets_msb_first_in_single_byte() {
        let mut bf = Bitfield::new(8);

        bf.set_bit(0).unwrap();
        assert_eq!(bf.data, vec![0b1000_0000]);
        assert!(bf.has_bit(0).unwrap());

        bf.set_bit(1).unwrap();
        assert_eq!(bf.data, vec![0b1100_0000]);
        assert!(bf.has_bit(1).unwrap());

        bf.set_bit(7).unwrap();
        assert_eq!(bf.data, vec![0b1100_0001]);
        assert!(bf.has_bit(7).unwrap());
    }

    #[test]
    fn set_bit_sets_across_byte_boundaries() {
        let mut bf = Bitfield::new(16);

        bf.set_bit(0).unwrap();
        bf.set_bit(7).unwrap();
        bf.set_bit(8).unwrap();
        bf.set_bit(15).unwrap();

        assert_eq!(bf.data, vec![0b1000_0001, 0b1000_0001]);
        assert!(bf.has_bit(0).unwrap());
        assert!(bf.has_bit(7).unwrap());
        assert!(bf.has_bit(8).unwrap());
        assert!(bf.has_bit(15).unwrap());
    }

    #[test]
    fn set_bit_is_idempotent() {
        let mut bf = Bitfield::new(8);

        bf.set_bit(3).unwrap();
        let first = bf.data.clone();

        bf.set_bit(3).unwrap();
        assert_eq!(bf.data, first);
    }

    #[test]
    fn extend_bytes_replaces_internal_storage() {
        let mut bf = Bitfield::new(8);

        bf.extend_bytes(&[0b1001_0011]).unwrap();

        assert_eq!(bf.data, vec![0b1001_0011]);
        assert!(bf.has_bit(0).unwrap());
        assert!(!bf.has_bit(1).unwrap());
        assert!(!bf.has_bit(2).unwrap());
        assert!(bf.has_bit(3).unwrap());
        assert!(!bf.has_bit(4).unwrap());
        assert!(!bf.has_bit(5).unwrap());
        assert!(bf.has_bit(6).unwrap());
        assert!(bf.has_bit(7).unwrap());
    }

    #[test]
    fn extend_bytes_works_for_multiple_bytes() {
        let mut bf = Bitfield::new(16);

        bf.extend_bytes(&[0b1010_0001, 0b0101_1000]).unwrap();

        assert!(bf.has_bit(0).unwrap());
        assert!(!bf.has_bit(1).unwrap());
        assert!(bf.has_bit(2).unwrap());
        assert!(!bf.has_bit(3).unwrap());
        assert!(!bf.has_bit(4).unwrap());
        assert!(!bf.has_bit(5).unwrap());
        assert!(!bf.has_bit(6).unwrap());
        assert!(bf.has_bit(7).unwrap());

        assert!(!bf.has_bit(8).unwrap());
        assert!(bf.has_bit(9).unwrap());
        assert!(!bf.has_bit(10).unwrap());
        assert!(bf.has_bit(11).unwrap());
        assert!(bf.has_bit(12).unwrap());
        assert!(!bf.has_bit(13).unwrap());
        assert!(!bf.has_bit(14).unwrap());
        assert!(!bf.has_bit(15).unwrap());
    }

    #[test]
    fn set_bit_out_of_range_returns_error() {
        let mut bf = Bitfield::new(8);

        assert!(matches!(bf.set_bit(8), Err(Error::PieceOutOfRange)));
        assert!(matches!(bf.set_bit(999), Err(Error::PieceOutOfRange)));
    }

    #[test]
    fn has_bit_out_of_range_returns_error() {
        let bf = Bitfield::new(8);

        assert!(matches!(bf.has_bit(8), Err(Error::PieceOutOfRange)));
        assert!(matches!(bf.has_bit(999), Err(Error::PieceOutOfRange)));
    }

    #[test]
    fn extend_bytes_wrong_length_returns_error() {
        let mut bf = Bitfield::new(8);

        assert!(matches!(bf.extend_bytes(&[]), Err(Error::PieceOutOfRange)));
        assert!(matches!(
            bf.extend_bytes(&[0, 0]),
            Err(Error::PieceOutOfRange)
        ));
    }

    #[test]
    fn partial_last_byte_only_allows_declared_pieces() {
        let mut bf = Bitfield::new(10);
        bf.extend_bytes(&[0b1111_1111, 0b1111_1111]).unwrap();

        for bit in 0..10 {
            assert!(bf.has_bit(bit).unwrap());
        }

        assert!(matches!(bf.has_bit(10), Err(Error::PieceOutOfRange)));
        assert!(matches!(bf.has_bit(15), Err(Error::PieceOutOfRange)));
    }
}
