use std::fmt;

use rand::TryRng;

use super::super::torrent::InfoHash;
use super::{Error, Result};

pub struct Handshake {
    pub info_hash: InfoHash,
    pub peer_id: PeerId,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerId([u8; 20]);

impl Handshake {
    pub const HANDSHAKE_LEN: usize = 68;
    const PSTR: &[u8; 19] = b"BitTorrent protocol";
    const RESERVED_LEN: usize = 8;

    pub fn new(info_hash: InfoHash, peer_id: PeerId) -> Self {
        Self { info_hash, peer_id }
    }

    pub fn encode(&self) -> [u8; Self::HANDSHAKE_LEN] {
        let mut out = [0u8; Self::HANDSHAKE_LEN];
        out[0] = Self::PSTR.len() as u8;
        out[1..20].copy_from_slice(Self::PSTR);
        out[20..28].copy_from_slice(&[0; Self::RESERVED_LEN]);
        out[28..48].copy_from_slice(self.info_hash.as_ref());
        out[48..68].copy_from_slice(self.peer_id.as_ref());
        out
    }

    pub(crate) fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() != Self::HANDSHAKE_LEN {
            return Err(Error::InvalidHandshake("invalid handshake length"));
        }

        let pstrlen = buf[0] as usize;
        if pstrlen != Self::PSTR.len() {
            return Err(Error::InvalidHandshake("invalid protocol string length"));
        }
        if &buf[1..20] != Self::PSTR {
            return Err(Error::InvalidHandshake("invalid protocol string"));
        }

        let mut hash_bytes = [0u8; 20];
        hash_bytes.copy_from_slice(&buf[28..48]);

        let mut peer_id_bytes = [0u8; 20];
        peer_id_bytes.copy_from_slice(&buf[48..68]);

        Ok(Self {
            info_hash: InfoHash::from(hash_bytes),
            peer_id: PeerId::new(peer_id_bytes),
        })
    }
}

impl PeerId {
    pub fn new(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }

    pub fn generate(client: &str, version: &str) -> Self {
        let mut id = [0u8; 20];

        let prefix = format!("-{}{}-", client, version);
        let prefix_bytes = prefix.as_bytes();

        let n = prefix_bytes.len().min(20);
        id[..n].copy_from_slice(&prefix_bytes[..n]);

        let mut rng = rand::rng();
        rng.try_fill_bytes(&mut id[n..]);

        Self(id)
    }
}

impl AsRef<[u8]> for PeerId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for &b in &self.0 {
            if b.is_ascii_graphic() || b == b' ' {
                write!(f, "{}", b as char)?;
            } else {
                write!(f, "\\x{b:02x}")?;
            }
        }
        Ok(())
    }
}

impl fmt::Debug for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PeerId({self})")
    }
}
