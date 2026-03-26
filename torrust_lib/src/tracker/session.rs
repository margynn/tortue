use super::{
    PeerId,
    client::TrackerClient,
    endpoint::TrackerEndpoint,
    error::Error,
    model::{AnnounceEvent, AnnounceRequest, SessionStats, TrackerResponse},
};

#[derive(Debug)]
pub struct TrackerSession {
    client: TrackerClient,
    request: AnnounceRequest,
    interval: Option<u32>,
    started: bool,
    completed_sent: bool,
}

impl TrackerSession {
    pub fn new(
        endpoint: TrackerEndpoint,
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
            completed_sent: false,
        })
    }

    pub fn stats(&self) -> SessionStats {
        self.request.stats
    }

    pub fn interval(&self) -> Option<u32> {
        self.interval
    }

    pub fn on_verified_download(&mut self, bytes: u64) {
        self.request.stats.downloaded = self.request.stats.downloaded.saturating_add(bytes);
        self.request.stats.left = self.request.stats.left.saturating_sub(bytes);
    }

    pub fn on_uploaded(&mut self, bytes: u64) {
        self.request.stats.uploaded = self.request.stats.uploaded.saturating_add(bytes);
    }

    pub async fn start(&mut self) -> Result<TrackerResponse, Error> {
        self.request.event = AnnounceEvent::Started;
        let resp = self.client.announce(&self.request).await?;
        self.interval = Some(resp.interval);
        self.started = true;
        self.request.event = AnnounceEvent::None;
        Ok(resp)
    }

    pub async fn reannounce(&mut self) -> Result<TrackerResponse, Error> {
        if !self.started {
            return self.start().await;
        }

        self.request.event = AnnounceEvent::None;
        let resp = self.client.announce(&self.request).await?;
        self.interval = Some(resp.interval);
        Ok(resp)
    }

    pub async fn complete(&mut self) -> Result<TrackerResponse, Error> {
        self.request.stats.left = 0;

        if self.completed_sent {
            return self.reannounce().await;
        }

        self.request.event = AnnounceEvent::Completed;
        let resp = self.client.announce(&self.request).await?;
        self.interval = Some(resp.interval);
        self.completed_sent = true;
        self.request.event = AnnounceEvent::None;
        Ok(resp)
    }

    pub async fn stop(&mut self) -> Result<(), Error> {
        self.request.event = AnnounceEvent::Stopped;
        let _ = self.client.announce(&self.request).await?;
        self.request.event = AnnounceEvent::None;
        Ok(())
    }
}
