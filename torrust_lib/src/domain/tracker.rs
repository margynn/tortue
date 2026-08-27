mod client;
mod session;

pub(crate) use client::TrackerAnnouncer;
pub(crate) use session::{Input, Output, TrackerSession};

use std::net::SocketAddr;

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

#[derive(Debug)]
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
