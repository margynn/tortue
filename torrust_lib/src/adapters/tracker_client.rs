use std::time::Duration;

use reqwest::Client;
use tokio::net::{UdpSocket, lookup_host};
use url::Url;

use crate::domain::tracker::{self, AnnounceRequest, TrackerResponse};

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
}

type Result<T> = std::result::Result<T, Error>;

pub enum TrackerClient {
    Http(HttpTransport),
    Udp(UdpTransport),
}

impl TrackerClient {
    pub fn new(s: &str) -> Result<Self> {
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

    pub async fn announce(&self, req: AnnounceRequest) -> Result<TrackerResponse> {
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

        let mut socket = UdpSocket::bind("[::]:0").await?;
        socket.connect(tracker_addr).await?;
        Ok(tracker::transport::udp::announce(&mut socket, req).await?)
    }
}
