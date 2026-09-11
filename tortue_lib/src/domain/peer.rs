use std::fmt;

use rand::TryRng;

use super::message::Message;

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
