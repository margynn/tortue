use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::time::sleep_until;

use super::{Error, PeerAddr, PeerId, TrackerClient};
use crate::torrent::InfoHash;

pub struct TrackerSession {
    client: TrackerClient,
    peers_tx: mpsc::Sender<Vec<PeerAddr>>,
    node: Node,
    torrent_info_hash: InfoHash,
    state: Arc<RwLock<State>>,
}

impl TrackerSession {
    pub fn new(
        endpoint: &str,
        torrent_info_hash: InfoHash,
        node: Node,
        content_size: u64,
        peers_tx: mpsc::Sender<Vec<PeerAddr>>,
    ) -> Result<Arc<Self>, Error> {
        let client = TrackerClient::new(endpoint)?;
        let state = Arc::new(RwLock::new(State {
            peers: Vec::new(),
            uploaded: 0,
            downloaded: 0,
            left: content_size,
            next_announce: Instant::now(),
            stopped: false,
            seeders: 0,
            leechers: 0,
        }));
        Ok(Arc::new(Self {
            client,
            peers_tx,
            torrent_info_hash,
            node,
            state,
        }))
    }

    pub fn start(self: Arc<Self>) {
        let this = Arc::clone(&self);

        tokio::spawn(async move {
            this.run_session().await;
        });
    }

    #[allow(dead_code)]
    pub fn peers(&self) -> Vec<PeerAddr> {
        self.state.read().unwrap().peers.clone()
    }

    #[allow(dead_code)]
    pub fn add_downloaded(&self, n: u64) {
        let mut s = self.state.write().unwrap();
        s.downloaded += n;
        s.left = s.left.saturating_sub(n);
    }

    #[allow(dead_code)]
    pub fn add_uploaded(&self, n: u64) {
        let mut s = self.state.write().unwrap();
        s.uploaded += n;
    }

    async fn run_session(self: Arc<Self>) {
        let mut event = Some(AnnounceEvent::Started);
        let mut backoff = Duration::from_secs(15);

        loop {
            let stopped = {
                let state = self.state.read().unwrap();
                state.stopped
            };
            if stopped {
                break;
            }

            let next = {
                let state = self.state.read().unwrap();
                state.next_announce
            };
            sleep_until(next.into()).await;

            let (uploaded, downloaded, left) = {
                let s = self.state.read().unwrap();
                (s.uploaded, s.downloaded, s.left)
            };

            let request = AnnounceRequest {
                info_hash: self.torrent_info_hash,
                peer_id: self.node.id,
                port: self.node.port,
                stats: SessionStats { uploaded, downloaded, left },
                event: event.take().unwrap_or(AnnounceEvent::None),
                compact: true,
            };

            match self.client.announce(&request).await {
                Ok(resp) => {
                    let now = Instant::now();
                    {
                        let mut s = self.state.write().unwrap();
                        s.peers = resp.peers.clone();
                        s.seeders = resp.seeders.unwrap_or_default();
                        s.leechers = resp.leechers.unwrap_or_default();

                        // schedule next announce (clamp 1min minimum)
                        let interval = resp.interval.max(60);
                        s.next_announce =
                            now + Duration::from_secs(interval as u64);
                    }

                    // reset backoff
                    backoff = Duration::from_secs(30);

                    let _ = self.peers_tx.send(resp.peers.clone()).await;
                },

                Err(_) => {
                    // failure → backoff
                    let mut s = self.state.write().unwrap();
                    s.next_announce = Instant::now() + backoff;
                    backoff = (backoff * 2).min(Duration::from_secs(3600));
                },
            }
        }
    }
}
