use super::super::torrent::InfoHash;
use super::{Error, PeerId, Result};

pub(crate) struct Handshake {
    info_hash: InfoHash,
    peer_id: PeerId,
}

impl Handshake {
    const PSTR: &[u8; 19] = b"BitTorrent protocol";
    pub(crate) const HANDSHAKE_LEN: usize = 68;
    const RESERVED_LEN: usize = 8;

    pub(crate) fn new(info_hash: InfoHash, peer_id: PeerId) -> Self {
        Self { info_hash, peer_id }
    }

    pub(crate) fn encode(&self) -> [u8; Self::HANDSHAKE_LEN] {
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
            return Err(Error::InvalidHandshake(
                "invalid protocol string length",
            ));
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
