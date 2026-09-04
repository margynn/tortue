use std::net::SocketAddr;
use std::time::Duration;

use reqwest::Client;
use tokio::net::{UdpSocket, lookup_host};
use tokio::sync::mpsc;
use tracing::{info, warn};
use url::Url;

use crate::domain::torrent::Metainfo;
use crate::domain::tracker::{
    self, AnnounceEvent, AnnounceRequest, Node, SessionStats, TrackerResponse,
};

const INITIAL_BACKOFF: Duration = Duration::from_secs(15);
const MAX_BACKOFF: Duration = Duration::from_secs(3600);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid http client")]
    InvalidHttpClient,

    #[error("http request: {0}")]
    HttpRequest(String),

    #[error("udp request: {0}")]
    UdpRequest(String),

    #[error("timeout")]
    Timeout,

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("tracker: {0}")]
    Tracker(#[from] tracker::transport::Error),

    #[error("invalid tracker url")]
    InvalidTrackerUrl,

    #[error("unsupported scheme: {0}")]
    UnsupportedScheme(String),

    #[error("missing udp host")]
    MissingUdpHost,

    #[error("missing udp port")]
    MissingUdpPort,

    #[error("peers channel closed")]
    PeersChannelClosed,
}

type Result<T> = std::result::Result<T, Error>;

pub struct TrackerIO {
    client: TrackerClient,
    metainfo: Metainfo,
    node: Node,
    peers_tx: mpsc::Sender<Vec<SocketAddr>>,
}

impl TrackerIO {
    pub fn new(
        url: &str,
        metainfo: Metainfo,
        node: Node,
        peers_tx: mpsc::Sender<Vec<SocketAddr>>,
    ) -> Result<Self> {
        let client = TrackerClient::new(url)?;
        Ok(Self { client, metainfo, node, peers_tx })
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("start_tracker"); // TODO: add tracker URL

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
                    left: self.metainfo.total_size(),
                },
                event: next_event.take().unwrap_or(AnnounceEvent::None),
                compact: true,
            };

            match self.client.announce(req).await {
                Ok(resp) => {
                    info!(
                        peers = resp.peers.len(),
                        interval = resp.interval,
                        "tracker announce succeeded"
                    );
                    backoff = INITIAL_BACKOFF;
                    interval = Duration::from_secs(resp.interval.max(60) as u64);
                    self.peers_tx.send(resp.peers).await.map_err(|_| Error::PeersChannelClosed)?;
                },
                Err(e) => {
                    warn!(error = %e, backoff_secs = backoff.as_secs(), "tracker announce failed");
                    interval = backoff;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                },
            }
        }
    }
}

enum TrackerClient {
    Http(HttpTransport),
    Udp(UdpTransport),
}

impl TrackerClient {
    fn new(s: &str) -> Result<Self> {
        let url = Url::parse(s).map_err(|_| Error::InvalidTrackerUrl)?;
        match url.scheme() {
            "http" | "https" => {
                let client = Client::builder()
                    .timeout(Duration::from_secs(10))
                    .pool_max_idle_per_host(8)
                    .pool_idle_timeout(Duration::from_secs(90))
                    .user_agent("ToRustLib/0.1.0")
                    .redirect(reqwest::redirect::Policy::limited(5))
                    .build()
                    .map_err(|_| Error::InvalidHttpClient)?;
                let transport = HttpTransport { client, url };
                Ok(Self::Http(transport))
            },

            "udp" => {
                let host = url.host_str().ok_or(Error::MissingUdpHost)?.to_owned();
                let port = url.port().ok_or(Error::MissingUdpPort)?;
                let transport = UdpTransport { host, port };
                Ok(Self::Udp(transport))
            },

            other => Err(Error::UnsupportedScheme(other.to_owned())),
        }
    }

    async fn announce(&self, req: AnnounceRequest) -> Result<TrackerResponse> {
        match self {
            Self::Http(t) => t.announce(&req).await,
            Self::Udp(t) => t.announce(&req).await,
        }
    }
}

struct HttpTransport {
    client: Client,
    url: Url,
}
struct UdpTransport {
    host: String,
    port: u16,
}

impl HttpTransport {
    async fn announce(&self, req: &AnnounceRequest) -> Result<TrackerResponse> {
        let url = tracker::transport::http::build_announce_url(&self.url, req);
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| Error::HttpRequest(e.to_string()))?;
        let bytes = response.bytes().await.map_err(|e| Error::HttpRequest(e.to_string()))?;
        Ok(tracker::transport::http::parse_response(&bytes)?)
    }
}

impl tracker::transport::UdpSocket for UdpSocket {
    async fn send(&self, buf: &[u8]) -> std::io::Result<()> {
        UdpSocket::send(self, buf).await.map(|_| ())
    }

    async fn recv(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        UdpSocket::recv(self, buf).await
    }
}

impl UdpTransport {
    async fn announce(&self, req: &AnnounceRequest) -> Result<TrackerResponse> {
        let tracker_addr = lookup_host((self.host.clone(), self.port))
            .await
            .map_err(|e| Error::UdpRequest(e.to_string()))?
            .next()
            .ok_or_else(|| {
                Error::UdpRequest("tracker hostname resolved to no address".to_owned())
            })?;

        let bind_addr = if tracker_addr.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" };
        let mut socket = UdpSocket::bind(bind_addr).await?;
        socket.connect(tracker_addr).await?;
        Ok(tracker::transport::udp::announce(&mut socket, req).await?)
    }
}
