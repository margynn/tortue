use super::{Error, Peer, PeerId, TrackerClient};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnounceEvent {
    Started,
    Completed,
    Stopped,
    None,
}

impl AnnounceEvent {
    pub fn as_http_str(&self) -> Option<&'static str> {
        match self {
            Self::Started => Some("started"),
            Self::Completed => Some("completed"),
            Self::Stopped => Some("stopped"),
            Self::None => None,
        }
    }

    pub fn as_udp_code(&self) -> u32 {
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
    request: AnnounceRequest,
    interval: Option<u32>,
    started: bool,
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
            request: AnnounceRequest {
                info_hash,
                peer_id,
                port,
                stats: SessionStats { uploaded: 0, downloaded: 0, left },
                event: AnnounceEvent::Started,
                compact: true,
            },
            interval: None,
            started: false,
        })
    }

    pub async fn start(&mut self) -> Result<TrackerResponse, Error> {
        self.request.event = AnnounceEvent::Started;
        let resp = self.client.announce(&self.request).await?;
        self.interval = Some(resp.interval);
        self.started = true;
        self.request.event = AnnounceEvent::None;
        Ok(resp)
    }

    // pub async fn reannounce(&mut self) -> Result<TrackerResponse, Error> {
    //     if !self.started {
    //         return self.start().await;
    //     }
    //     self.request.event = AnnounceEvent::None;
    //     let resp = self.client.announce(&self.request).await?;
    //     self.interval = Some(resp.interval);
    //     Ok(resp)
    // }

    // pub async fn complete(&mut self) -> Result<TrackerResponse, Error> {
    //     self.request.stats.left = 0;
    //     if self.completed_sent {
    //         return self.reannounce().await;
    //     }
    //     self.request.event = AnnounceEvent::Completed;
    //     let resp = self.client.announce(&self.request).await?;
    //     self.interval = Some(resp.interval);
    //     self.completed_sent = true;
    //     self.request.event = AnnounceEvent::None;
    //     Ok(resp)
    // }

    // pub async fn stop(&mut self) -> Result<(), Error> {
    //     self.request.event = AnnounceEvent::Stopped;
    //     let _ = self.client.announce(&self.request).await?;
    //     self.request.event = AnnounceEvent::None;
    //     Ok(())
    // }
}
