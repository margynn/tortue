use std::net::IpAddr;

use rand::TryRng;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct PeerAddr(pub IpAddr, pub u16);

// #[derive(Debug, Clone, PartialEq, Eq, Hash)]
// pub struct Peer {
//     pub peer_id: Option<PeerId>,
//     pub ip: IpAddr,
//     pub port: u16,
// }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

        // new API in rand 0.9
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
