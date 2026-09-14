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

#[derive(Clone, Copy)]
pub struct SessionStats {
    pub uploaded: usize,
    pub downloaded: usize,
    pub left: usize,
}

pub struct TrackerResponse {
    pub interval: u32,
    pub peers: Vec<SocketAddr>,
}

#[derive(Clone, Copy)]
pub struct Node {
    pub id: PeerId,
    pub port: u16,
}
