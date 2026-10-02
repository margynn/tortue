use std::fmt;

use rand::TryRng;

use super::message::Message;
use super::torrent::InfoHash;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid handshake: {0}")]
    InvalidHandshake(&'static str),
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum PeerEvent {
    Connected {
        peer_id: PeerId,
        peer_extensions: PeerExtensions,
    },
    Disconnected,
    MessageReceived(Message),
}

#[derive(Debug, Clone, Copy)]
pub struct PeerExtensions {
    pub dht: bool,  // BEP 5
    pub fast: bool, // BEP 6
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerId([u8; 20]);

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
        rand::rng().try_fill_bytes(&mut id[n..]);
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

pub struct Handshake {
    pub info_hash: InfoHash,
    pub peer_id: PeerId,
    pub dht_protocol: bool,
    pub extension_protocol: bool,
    pub fast_extension: bool,
}

impl Handshake {
    // BitTorrent handshake (BEP 3).
    //
    // Offset  Size  Field
    // ------  ----  ------------------------------------------------
    // 0       1     pstrlen      = 19
    // 1       19    pstr         = "BitTorrent protocol"
    // 20      8     reserved     Extension / feature flags
    // 28      20    info_hash    SHA-1 hash of the torrent info dictionary
    // 48      20    peer_id      Peer identifier
    //
    // Total size: 68 bytes.
    //
    // `reserved` bits commonly used:
    //
    // reserved[5] bit 4 (0x10) → BEP 10: Extension Protocol
    // reserved[7] bit 2 (0x04) → BEP 6:  Fast Extension
    // reserved[7] bit 0 (0x01) → BEP 5:  DHT Protocol

    pub const HANDSHAKE_LEN: usize = 68;
    const PSTR: &[u8; 19] = b"BitTorrent protocol";

    const EXTENSION_PROTOCOL_MASK: u8 = 0b0001_0000;
    const FAST_EXTENSION_MASK: u8 = 0b0000_0100;
    const DHT_PROTOCOL_MASK: u8 = 0b0000_0001;

    pub fn new(
        info_hash: InfoHash,
        peer_id: PeerId,
        dht_protocol: bool,
        extension_protocol: bool,
        fast_extension: bool,
    ) -> Self {
        Self {
            info_hash,
            peer_id,
            fast_extension,
            extension_protocol,
            dht_protocol,
        }
    }

    pub fn encode(&self) -> [u8; Self::HANDSHAKE_LEN] {
        let mut out = [0u8; Self::HANDSHAKE_LEN];
        out[0] = Self::PSTR.len() as u8;
        out[1..20].copy_from_slice(Self::PSTR);
        if self.extension_protocol {
            out[25] |= Self::EXTENSION_PROTOCOL_MASK;
        }
        if self.fast_extension {
            out[27] |= Self::FAST_EXTENSION_MASK;
        }
        if self.dht_protocol {
            out[27] |= Self::DHT_PROTOCOL_MASK;
        }
        out[28..48].copy_from_slice(self.info_hash.as_ref());
        out[48..68].copy_from_slice(self.peer_id.as_ref());
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() != Self::HANDSHAKE_LEN {
            return Err(Error::InvalidHandshake("invalid handshake length"));
        }
        if buf[0] as usize != Self::PSTR.len() {
            return Err(Error::InvalidHandshake("invalid protocol string length"));
        }
        if &buf[1..20] != Self::PSTR {
            return Err(Error::InvalidHandshake("invalid protocol string"));
        }

        let mut reserved_bytes = [0u8; 8];
        reserved_bytes.copy_from_slice(&buf[20..28]);

        let mut hash_bytes = [0u8; 20];
        hash_bytes.copy_from_slice(&buf[28..48]);

        let mut peer_id_bytes = [0u8; 20];
        peer_id_bytes.copy_from_slice(&buf[48..68]);

        let extension_protocol = (reserved_bytes[5] & Self::EXTENSION_PROTOCOL_MASK) != 0;
        let fast_extension = (reserved_bytes[7] & Self::FAST_EXTENSION_MASK) != 0;
        let dht_protocol = (reserved_bytes[7] & Self::DHT_PROTOCOL_MASK) != 0;

        Ok(Handshake::new(
            InfoHash::from(hash_bytes),
            PeerId::new(peer_id_bytes),
            dht_protocol,
            extension_protocol,
            fast_extension,
        ))
    }
}
