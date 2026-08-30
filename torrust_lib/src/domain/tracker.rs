pub mod http;
mod session;
pub mod udp;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

pub use session::{Input, Output, TrackerSession};

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("bencode: {0}")]
    Bencode(#[from] crate::domain::bencode::Error),

    #[error("tracker failure: {0}")]
    TrackerFailure(String),

    #[error("invalid response: {0}")]
    InvalidResponse(String),
}

use super::peer::PeerId;
use super::torrent::InfoHash;

#[derive(Debug)]
pub struct AnnounceRequest {
    pub info_hash: InfoHash,
    pub peer_id: PeerId,
    pub port: u16,
    pub stats: SessionStats,
    pub event: AnnounceEvent,
    pub compact: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum AnnounceEvent {
    Started,
    Completed,
    Stopped,
    None,
}

impl AnnounceEvent {
    pub fn as_http_str(self) -> Option<&'static str> {
        match self {
            Self::Started => Some("started"),
            Self::Completed => Some("completed"),
            Self::Stopped => Some("stopped"),
            Self::None => None,
        }
    }

    pub fn as_udp_code(self) -> u32 {
        match self {
            Self::None => 0,
            Self::Completed => 1,
            Self::Started => 2,
            Self::Stopped => 3,
        }
    }
}

#[derive(Debug)]
pub struct SessionStats {
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
}

#[derive(Debug, Clone)]
pub struct TrackerResponse {
    pub interval: u32,
    pub peers: Vec<SocketAddr>,
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
}

#[derive(Clone, Copy)]
pub struct Node {
    pub id: PeerId,
    pub port: u16,
}

type Result<T> = std::result::Result<T, Error>;

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
