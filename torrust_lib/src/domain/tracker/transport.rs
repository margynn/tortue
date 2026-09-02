use std::io;
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

    #[error("io: {0}")]
    Io(io::Error),
}
type Result<T> = std::result::Result<T, Error>;

pub trait UdpSocket {
    async fn send(&self, buf: &[u8]) -> io::Result<()>;
    async fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize>;
}

fn parse_compact_ipv4_peers(bytes: &[u8]) -> Result<Vec<SocketAddr>> {
    if !bytes.len().is_multiple_of(6) {
        return Err(Error::InvalidResponse(
            "compact ipv4 peers length must be multiple of 6".to_owned(),
        ));
    }
    let mut peers = Vec::with_capacity(bytes.len() / 6);
    for chunk in bytes.chunks_exact(6) {
        let ip = IpAddr::V4(Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]));
        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
        peers.push(SocketAddr::new(ip, port));
    }
    Ok(peers)
}
