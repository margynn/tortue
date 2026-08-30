use std::net::{IpAddr, Ipv4Addr, SocketAddr};

pub mod http;
pub mod udp;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("bencode: {0}")]
    Bencode(#[from] crate::domain::bencode::Error),

    #[error("tracker failure: {0}")]
    TrackerFailure(String),

    #[error("invalid response: {0}")]
    InvalidResponse(String),
}
type Result<T> = std::result::Result<T, Error>;

trait TrackerTransport {
    async fn connect(&mut self) -> Result<()>;
    async fn send(&mut self, data: &[u8]) -> Result<()>;
    async fn recv(&mut self) -> Result<Vec<u8>>;
}

fn parse_compact_ipv4_peers(bytes: &[u8]) -> Result<Vec<SocketAddr>> {
    if !bytes.len().is_multiple_of(6) {
        return Err(Error::InvalidResponse(
            "compact ipv4 peers length must be multiple of 6".to_owned(),
        ));
    }
    let mut peers = Vec::with_capacity(bytes.len() / 6);
    for chunk in bytes.chunks_exact(6) {
        let ip =
            IpAddr::V4(Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]));
        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
        peers.push(SocketAddr::new(ip, port));
    }
    Ok(peers)
}
