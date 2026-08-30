use std::time::Duration;

use reqwest::Client;
use tokio::net::{UdpSocket, lookup_host};
use tokio::time::timeout;
use url::Url;

use crate::domain::tracker::{self, AnnounceRequest, TrackerResponse};

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("invalid tracker url")]
    InvalidTrackerUrl,

    #[error("unsupported scheme: {0}")]
    UnsupportedScheme(String),

    #[error("missing udp host")]
    MissingUdpHost,

    #[error("missing udp port")]
    MissingUdpPort,

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

    #[error("parse: {0}")]
    Parse(#[from] tracker::Error),
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
enum Endpoint {
    Http { url: Url },
    Udp { host: String, port: u16 },
}

impl Endpoint {
    fn parse(s: &str) -> Result<Self> {
        let url = Url::parse(s).map_err(|_| Error::InvalidTrackerUrl)?;
        match url.scheme() {
            "http" | "https" => Ok(Self::Http { url }),
            "udp" => {
                let host =
                    url.host_str().ok_or(Error::MissingUdpHost)?.to_owned();
                let port = url.port().ok_or(Error::MissingUdpPort)?;
                Ok(Self::Udp { host, port })
            },
            other => Err(Error::UnsupportedScheme(other.to_owned())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TrackerIO {
    endpoint: Endpoint,
    http: Client,
}

impl TrackerIO {
    pub fn new(s: &str) -> Option<Self> {
        let endpoint = Endpoint::parse(s).ok()?;
        let http = Client::builder()
            .timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(8)
            .pool_idle_timeout(Duration::from_secs(90))
            .user_agent("ToRustLib/0.1.0")
            .redirect(reqwest::redirect::Policy::limited(5))
            .build()
            .map_err(|_| Error::InvalidHttpClient)
            .ok()?;
        Some(Self { endpoint, http })
    }

    async fn announce(&self, req: &AnnounceRequest) -> Option<TrackerResponse> {
        match &self.endpoint {
            Endpoint::Http { url } => {
                http_announce(&self.http, url, req).await.ok()
            },
            Endpoint::Udp { host, port } => {
                udp_announce(host, *port, req).await.ok()
            },
        }
    }
}

async fn http_announce(
    client: &Client,
    base_url: &Url,
    req: &AnnounceRequest,
) -> Result<TrackerResponse> {
    let url = tracker::http::build_announce_url(base_url, req);
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| Error::HttpRequest(e.to_string()))?;
    let bytes = response
        .bytes()
        .await
        .map_err(|e| Error::HttpRequest(e.to_string()))?;
    Ok(tracker::http::parse_response(&bytes)?)
}

async fn udp_announce(
    host: &str,
    port: u16,
    req: &AnnounceRequest,
) -> Result<TrackerResponse> {
    let tracker_addr = lookup_host((host, port))
        .await
        .map_err(|e| Error::UdpRequest(e.to_string()))?
        .next()
        .ok_or_else(|| {
            Error::UdpRequest(
                "tracker hostname resolved to no address".to_owned(),
            )
        })?;

    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(tracker_addr).await?;

    let tx_id = rand::random::<u32>();
    socket.send(&tracker::udp::build_connect_request(tx_id)).await?;

    let mut buf = [0u8; 4096];
    let n = timeout(Duration::from_secs(2), socket.recv(&mut buf))
        .await
        .map_err(|_| Error::Timeout)??;
    let connection_id = tracker::udp::parse_connect_response(&buf[..n], tx_id)?;

    let announce_tx = rand::random::<u32>();
    let key = rand::random::<u32>();
    socket
        .send(&tracker::udp::build_announce_request(
            connection_id,
            announce_tx,
            key,
            req,
        ))
        .await?;

    let n = socket.recv(&mut buf).await?;
    Ok(tracker::udp::parse_announce_response(&buf[..n], announce_tx)?)
}
