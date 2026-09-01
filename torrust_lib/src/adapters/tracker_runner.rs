use std::net::SocketAddr;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::debug;

use crate::adapters::tracker_client::TrackerClient;
use crate::domain::torrent::Metainfo;
use crate::domain::tracker::{AnnounceEvent, AnnounceRequest, Node, SessionStats};

const INITIAL_BACKOFF: Duration = Duration::from_secs(15);
const MAX_BACKOFF: Duration = Duration::from_secs(3600);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("peers channel closed")]
    PeersChannelClosed,
}

type Result<T> = std::result::Result<T, Error>;

pub struct TrackerRunner {
    client: TrackerClient,
    metainfo: Metainfo,
    node: Node,
    peers_tx: mpsc::Sender<Vec<SocketAddr>>,
}

impl TrackerRunner {
    pub fn new(
        client: TrackerClient,
        metainfo: Metainfo,
        node: Node,
        peers_tx: mpsc::Sender<Vec<SocketAddr>>,
    ) -> Self {
        Self { client, metainfo, node, peers_tx }
    }

    pub async fn run(&mut self) -> Result<()> {
        debug!("start_tracker"); // TODO: add tracker URL

        let mut interval = Duration::ZERO;
        let mut backoff = INITIAL_BACKOFF;
        let mut next_event = Some(AnnounceEvent::Started);

        loop {
            tokio::time::sleep(interval).await;

            let req = AnnounceRequest {
                info_hash: self.metainfo.hash,
                peer_id: self.node.id,
                port: self.node.port,
                stats: SessionStats {
                    // TODO: use an Arc<SessionStats> to share ?
                    uploaded: 0,   // TODO: should update
                    downloaded: 0, // TODO: should update
                    left: self.metainfo.size(),
                },
                event: next_event.take().unwrap_or(AnnounceEvent::None),
                compact: true,
            };

            match self.client.announce(req).await {
                Ok(resp) => {
                    backoff = INITIAL_BACKOFF;
                    interval = Duration::from_secs(resp.interval.max(60) as u64);
                    self.peers_tx.send(resp.peers).await.map_err(|_| Error::PeersChannelClosed)?;
                },
                Err(_) => {
                    interval = backoff;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                },
            }
        }
    }
}
