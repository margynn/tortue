use std::net::SocketAddr;

use super::swarm::SwarmStatus;
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
}

#[derive(Clone, Copy)]
pub struct SessionStats {
    pub swarm_status: SwarmStatus,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
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

impl Node {
    // TODO: get from config
    const CLIENT: &str = "TT"; // Tortue Client
    const VERSION: &str = "0.1.0";

    pub fn new() -> Self {
        Self {
            id: PeerId::generate(Self::CLIENT, Self::VERSION),
            port: 1234,
        }
    }
}
