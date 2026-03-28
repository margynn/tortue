use super::{Error, Peer, PeerId, TrackerClient};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, Copy, Default)]
pub struct SessionStats {
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
}

#[derive(Debug, Clone)]
pub struct AnnounceRequest {
    pub info_hash: [u8; 20],
    pub peer_id: PeerId,
    pub port: u16,
    pub stats: SessionStats,
    pub event: AnnounceEvent,
    pub compact: bool,
}

#[derive(Debug, Clone)]
pub struct TrackerResponse {
    pub interval: u32,
    pub peers: Vec<Peer>,
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
}

#[derive(Debug)]
pub struct TrackerSession {
    client: TrackerClient,
    info_hash: [u8; 20],
    peer_id: PeerId,
    port: u16,
    left: u64,
}

impl TrackerSession {
    pub fn new(
        endpoint: &str,
        info_hash: [u8; 20],
        peer_id: PeerId,
        port: u16,
        left: u64,
    ) -> Result<Self, Error> {
        Ok(Self {
            client: TrackerClient::new(endpoint)?,
            info_hash,
            peer_id,
            port,
            left,
        })
    }

    pub async fn announce_started(&self) -> Result<TrackerResponse, Error> {
        let request = AnnounceRequest {
            info_hash: self.info_hash,
            peer_id: self.peer_id,
            port: self.port,
            stats: SessionStats {
                uploaded: 0,
                downloaded: 0,
                left: self.left,
            },
            event: AnnounceEvent::Started,
            compact: true,
        };

        self.client.announce(&request).await
    }
}
