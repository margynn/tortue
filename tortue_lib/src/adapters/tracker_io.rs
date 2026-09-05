use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_encode};
use reqwest::Client;
use tokio::{
    net::{UdpSocket as TokioUdpSocket, lookup_host},
    sync::mpsc,
};
use tracing::{info, warn};
use url::Url;

use crate::{
    adapters::bencode::Bencode,
    application::ports::peer_source::PeerSource,
    domain::{
        torrent::Metainfo,
        tracker::{AnnounceEvent, AnnounceRequest, Node, SessionStats, TrackerResponse},
    },
};

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid http client")]
    InvalidHttpClient,

    #[error("http request: {0}")]
    HttpRequest(String),

    #[error("udp request: {0}")]
    UdpRequest(String),

    #[error("bencode: {0}")]
    Bencode(#[from] crate::adapters::bencode::Error),

    #[error("tracker failure: {0}")]
    TrackerFailure(String),

    #[error("invalid response: {0}")]
    InvalidResponse(String),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

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

// ── TrackerIO ─────────────────────────────────────────────────────────────────

pub struct TrackerIO {
    client: TrackerClient,
    metainfo: Arc<Metainfo>,
    node: Node,
}

impl TrackerIO {
    const INITIAL_BACKOFF: Duration = Duration::from_secs(15);
    const MAX_BACKOFF: Duration = Duration::from_secs(3600);

    pub fn new(url: &str, metainfo: Arc<Metainfo>, node: Node) -> Result<Self> {
        let client = TrackerClient::new(url)?;
        Ok(Self {
            client,
            metainfo,
            node,
        })
    }
}

impl PeerSource for TrackerIO {
    async fn run(self, tx: mpsc::Sender<Vec<SocketAddr>>) -> anyhow::Result<()> {
        info!("start_tracker");

        let mut interval = Duration::ZERO;
        let mut backoff = Self::INITIAL_BACKOFF;
        let mut next_event = Some(AnnounceEvent::Started);

        loop {
            tokio::time::sleep(interval).await;

            let req = AnnounceRequest {
                info_hash: self.metainfo.hash,
                peer_id: self.node.id,
                port: self.node.port,
                // TODO: update
                stats: SessionStats {
                    uploaded: 0,
                    downloaded: 0,
                    left: self.metainfo.total_size(),
                },
                event: next_event.take().unwrap_or(AnnounceEvent::None),
                compact: true,
            };

            match self.client.announce(&req).await {
                Ok(resp) => {
                    info!(
                        peers = resp.peers.len(),
                        interval = resp.interval,
                        "tracker announce succeeded"
                    );
                    backoff = Self::INITIAL_BACKOFF;
                    interval = Duration::from_secs(resp.interval.max(60) as u64);
                    if tx.send(resp.peers).await.is_err() {
                        return Ok(());
                    }
                },
                Err(e) => {
                    warn!(error = %e, backoff_secs = backoff.as_secs(), "tracker announce failed");
                    interval = backoff;
                    backoff = (backoff * 2).min(Self::MAX_BACKOFF);
                },
            }
        }
    }
}

// ── Transport trait ───────────────────────────────────────────────────────────

trait Transport {
    async fn announce(&self, req: &AnnounceRequest) -> Result<TrackerResponse>;
}

// ── TrackerClient ─────────────────────────────────────────────────────────────

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
                Ok(Self::Http(HttpTransport { client, url }))
            },
            "udp" => {
                let host = url.host_str().ok_or(Error::MissingUdpHost)?.to_owned();
                let port = url.port().ok_or(Error::MissingUdpPort)?;
                Ok(Self::Udp(UdpTransport { host, port }))
            },
            other => Err(Error::UnsupportedScheme(other.to_owned())),
        }
    }
}

impl Transport for TrackerClient {
    async fn announce(&self, req: &AnnounceRequest) -> Result<TrackerResponse> {
        match self {
            Self::Http(t) => Transport::announce(t, req).await,
            Self::Udp(t) => Transport::announce(t, req).await,
        }
    }
}

// ── HTTP transport ────────────────────────────────────────────────────────────

struct HttpTransport {
    client: Client,
    url: Url,
}

impl Transport for HttpTransport {
    async fn announce(&self, req: &AnnounceRequest) -> Result<TrackerResponse> {
        let url = self.build_announce_url(req);
        let bytes = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| Error::HttpRequest(e.to_string()))?
            .bytes()
            .await
            .map_err(|e| Error::HttpRequest(e.to_string()))?;
        self.parse_response(&bytes)
    }
}

impl HttpTransport {
    const ENCODE_SET: &'static AsciiSet = NON_ALPHANUMERIC;

    fn build_announce_url(&self, req: &AnnounceRequest) -> String {
        let mut url = self.url.clone();
        {
            let mut qp = url.query_pairs_mut();
            qp.append_pair("port", &req.port.to_string());
            qp.append_pair("uploaded", &req.stats.uploaded.to_string());
            qp.append_pair("downloaded", &req.stats.downloaded.to_string());
            qp.append_pair("left", &req.stats.left.to_string());
            qp.append_pair("compact", if req.compact { "1" } else { "0" });
            if let Some(event) = req.event.as_http_str() {
                qp.append_pair("event", event);
            }
        }
        let sep = if url.query().is_some() { "&" } else { "?" };
        let mut out: String = url.into();
        out.push_str(sep);
        out.push_str("info_hash=");
        out.push_str(&percent_encode(req.info_hash.as_ref(), Self::ENCODE_SET).to_string());
        out.push_str("&peer_id=");
        out.push_str(&percent_encode(req.peer_id.as_ref(), Self::ENCODE_SET).to_string());
        out
    }

    fn parse_response(&self, bytes: &[u8]) -> Result<TrackerResponse> {
        let decoded = Bencode::decode(bytes)?;

        if let Ok(reason) = decoded.get_bytes(b"failure reason") {
            return Err(Error::TrackerFailure(
                String::from_utf8_lossy(reason).into_owned(),
            ));
        }

        let interval = u32::try_from(decoded.get_int(b"interval")?)
            .map_err(|_| Error::InvalidResponse("interval out of range".to_owned()))?;

        let peers = match (decoded.get_bytes(b"peers"), decoded.get_list(b"peers")) {
            (Ok(compact), _) => parse_compact_ipv4_peers(compact)?,
            (_, Ok(list)) => Self::parse_peers_list(list)?,
            _ => return Err(Error::InvalidResponse("missing peers field".to_owned())),
        };

        Ok(TrackerResponse { interval, peers })
    }

    fn parse_peers_list(peer_list: &[Bencode<'_>]) -> Result<Vec<SocketAddr>> {
        peer_list
            .iter()
            .map(|peer| {
                let ip_raw = peer.get_bytes(b"ip")?;
                let ip_str = std::str::from_utf8(ip_raw).map_err(|_| {
                    Error::InvalidResponse("peer ip was not valid utf-8".to_owned())
                })?;
                let ip: IpAddr = ip_str
                    .parse()
                    .map_err(|_| Error::InvalidResponse(format!("invalid peer ip: {ip_str}")))?;
                let port = u16::try_from(peer.get_int(b"port")?)
                    .map_err(|_| Error::InvalidResponse("peer port out of range".to_owned()))?;
                Ok(SocketAddr::new(ip, port))
            })
            .collect()
    }
}

// ── UDP transport ─────────────────────────────────────────────────────────────

struct UdpTransport {
    host: String,
    port: u16,
}

impl Transport for UdpTransport {
    async fn announce(&self, req: &AnnounceRequest) -> Result<TrackerResponse> {
        let tracker_addr = lookup_host((self.host.clone(), self.port))
            .await
            .map_err(|e| Error::UdpRequest(e.to_string()))?
            .next()
            .ok_or_else(|| {
                Error::UdpRequest("tracker hostname resolved to no address".to_owned())
            })?;

        let bind_addr = if tracker_addr.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        let socket = TokioUdpSocket::bind(bind_addr).await?;
        socket.connect(tracker_addr).await?;

        let mut buf = [0u8; 4096];

        let connect_tx = rand::random::<u32>();
        socket
            .send(&Self::build_connect_request(connect_tx))
            .await?;
        let n = socket.recv(&mut buf).await?;
        let connection_id = Self::parse_connect_response(&buf[..n], connect_tx)?;

        let announce_tx = rand::random::<u32>();
        socket
            .send(&Self::build_announce_request(
                connection_id,
                announce_tx,
                rand::random(),
                req,
            ))
            .await?;
        let n = socket.recv(&mut buf).await?;
        Self::parse_announce_response(&buf[..n], announce_tx)
    }
}

impl UdpTransport {
    const PROTOCOL_ID: u64 = 0x41727101980;
    const ACTION_CONNECT: u32 = 0;
    const ACTION_ANNOUNCE: u32 = 1;

    fn build_connect_request(tx_id: u32) -> [u8; 16] {
        let mut buf = [0u8; 16];
        buf[0..8].copy_from_slice(&Self::PROTOCOL_ID.to_be_bytes());
        buf[8..12].copy_from_slice(&Self::ACTION_CONNECT.to_be_bytes());
        buf[12..16].copy_from_slice(&tx_id.to_be_bytes());
        buf
    }

    fn parse_connect_response(bytes: &[u8], expected_tx_id: u32) -> Result<u64> {
        if bytes.len() < 16 {
            return Err(Error::InvalidResponse(
                "udp connect response too short".to_owned(),
            ));
        }
        let action = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let tx_id = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
        if action != Self::ACTION_CONNECT {
            return Err(Error::InvalidResponse(format!(
                "unexpected udp connect action: {action}"
            )));
        }
        if tx_id != expected_tx_id {
            return Err(Error::InvalidResponse(
                "udp connect transaction id mismatch".to_owned(),
            ));
        }
        Ok(u64::from_be_bytes(bytes[8..16].try_into().unwrap()))
    }

    fn build_announce_request(
        connection_id: u64,
        tx_id: u32,
        key: u32,
        req: &AnnounceRequest,
    ) -> [u8; 98] {
        let mut buf = [0u8; 98];
        buf[0..8].copy_from_slice(&connection_id.to_be_bytes());
        buf[8..12].copy_from_slice(&Self::ACTION_ANNOUNCE.to_be_bytes());
        buf[12..16].copy_from_slice(&tx_id.to_be_bytes());
        buf[16..36].copy_from_slice(req.info_hash.as_ref());
        buf[36..56].copy_from_slice(req.peer_id.as_ref());
        buf[56..64].copy_from_slice(&req.stats.downloaded.to_be_bytes());
        buf[64..72].copy_from_slice(&req.stats.left.to_be_bytes());
        buf[72..80].copy_from_slice(&req.stats.uploaded.to_be_bytes());
        buf[80..84].copy_from_slice(&req.event.as_udp_code().to_be_bytes());
        buf[84..88].copy_from_slice(&0u32.to_be_bytes());
        buf[88..92].copy_from_slice(&key.to_be_bytes());
        buf[92..96].copy_from_slice(&(-1i32).to_be_bytes());
        buf[96..98].copy_from_slice(&req.port.to_be_bytes());
        buf
    }

    fn parse_announce_response(bytes: &[u8], expected_tx_id: u32) -> Result<TrackerResponse> {
        if bytes.len() < 20 {
            return Err(Error::InvalidResponse(
                "udp announce response too short".to_owned(),
            ));
        }
        let action = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let tx_id = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
        if action != Self::ACTION_ANNOUNCE {
            return Err(Error::InvalidResponse(format!(
                "unexpected udp announce action: {action}"
            )));
        }
        if tx_id != expected_tx_id {
            return Err(Error::InvalidResponse(
                "udp announce transaction id mismatch".to_owned(),
            ));
        }
        let interval = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        // let leechers = u32::from_be_bytes(bytes[12..16].try_into().unwrap());
        // let seeders = u32::from_be_bytes(bytes[16..20].try_into().unwrap());
        let peers = parse_compact_ipv4_peers(&bytes[20..])?;
        Ok(TrackerResponse { interval, peers })
    }
}

// ── AnnounceEvent wire mappings ───────────────────────────────────────────────

impl AnnounceEvent {
    fn as_http_str(self) -> Option<&'static str> {
        match self {
            Self::Started => Some("started"),
            Self::Completed => Some("completed"),
            Self::Stopped => Some("stopped"),
            Self::None => None,
        }
    }

    fn as_udp_code(self) -> u32 {
        match self {
            Self::None => 0,
            Self::Completed => 1,
            Self::Started => 2,
            Self::Stopped => 3,
        }
    }
}

// ── Shared wire helpers ───────────────────────────────────────────────────────

fn parse_compact_ipv4_peers(bytes: &[u8]) -> Result<Vec<SocketAddr>> {
    if !bytes.len().is_multiple_of(6) {
        return Err(Error::InvalidResponse(
            "compact ipv4 peers length must be multiple of 6".to_owned(),
        ));
    }
    Ok(bytes
        .chunks_exact(6)
        .map(|c| {
            let ip = IpAddr::V4(Ipv4Addr::new(c[0], c[1], c[2], c[3]));
            let port = u16::from_be_bytes([c[4], c[5]]);
            SocketAddr::new(ip, port)
        })
        .collect())
}
