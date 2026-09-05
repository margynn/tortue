use std::net::SocketAddr;

use super::{peer::PeerId, torrent::InfoHash};

pub struct AnnounceRequest {
    pub info_hash: InfoHash,
    pub peer_id: PeerId,
    pub port: u16,
    pub stats: SessionStats,
    pub event: AnnounceEvent,
    pub compact: bool,
}

#[derive(Clone, Copy)]
pub enum AnnounceEvent {
    Started,
    Completed,
    Stopped,
    None,
}

pub struct SessionStats {
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
}

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
