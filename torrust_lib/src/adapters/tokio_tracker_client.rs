use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_encode};
use reqwest::Client;
use tokio::net::{UdpSocket, lookup_host};
use tokio::time::timeout;
use url::Url;

use crate::domain::bencode::Bencode;
use crate::domain::tracker::{
    AnnounceRequest, TrackerAnnouncer, TrackerResponse,
};

#[derive(Debug)]
enum Error {
    InvalidTrackerUrl,
    UnsupportedScheme(String),
    MissingUdpHost,
    MissingUdpPort,
    InvalidHttpClient,
    HttpRequest(String),
    UdpRequest(String),
    TrackerFailure(String),
    InvalidTrackerResponse(String),
    Timeout,
    Bencode(crate::domain::bencode::Error),
    Io(std::io::Error),
}

impl From<crate::domain::bencode::Error> for Error {
    fn from(e: crate::domain::bencode::Error) -> Self {
        Self::Bencode(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
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
pub struct TokioTrackerClient {
    endpoint: Endpoint,
    http: Client,
}

impl TokioTrackerClient {
    pub fn new(s: &str) -> Option<Self> {
        let endpoint = Endpoint::parse(s).ok()?;
        let http = Client::builder()
            .timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(8)
            .pool_idle_timeout(Duration::from_secs(90))
            .user_agent("ToRustLib/0.1.0")
            .redirect(reqwest::redirect::Policy::limited(5))
            .build()
            .ok()?;
        Some(Self { endpoint, http })
    }
}

impl TrackerAnnouncer for TokioTrackerClient {
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

// --- HTTP ---

const TRACKER_ENCODE_SET: &AsciiSet = NON_ALPHANUMERIC;

async fn http_announce(
    client: &Client,
    base_url: &Url,
    req: &AnnounceRequest,
) -> Result<TrackerResponse> {
    let url = build_http_url(base_url, req);
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| Error::HttpRequest(e.to_string()))?;
    let bytes = response
        .bytes()
        .await
        .map_err(|e| Error::HttpRequest(e.to_string()))?;
    let decoded = Bencode::decode(&bytes)?;

    if let Ok(reason) = decoded.get_bytes(b"failure reason") {
        return Err(Error::TrackerFailure(
            String::from_utf8_lossy(reason).into_owned(),
        ));
    }

    let interval_i64 = decoded.get_int(b"interval")?;
    let interval = u32::try_from(interval_i64).map_err(|_| {
        Error::InvalidTrackerResponse("interval out of range".to_owned())
    })?;

    let peers = match (decoded.get_bytes(b"peers"), decoded.get_list(b"peers"))
    {
        (Ok(compact), _) => parse_compact_ipv4_peers(compact)?,
        (_, Ok(list)) => parse_http_peers_list(list)?,
        _ => {
            return Err(Error::InvalidTrackerResponse(
                "missing peers field".to_owned(),
            ));
        },
    };

    Ok(TrackerResponse {
        interval,
        peers,
        seeders: None,
        leechers: None,
    })
}

fn build_http_url(base_url: &Url, req: &AnnounceRequest) -> String {
    let mut url = base_url.clone();
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
    out.push_str(
        &percent_encode(req.info_hash.as_ref(), TRACKER_ENCODE_SET).to_string(),
    );
    out.push_str("&peer_id=");
    out.push_str(
        &percent_encode(req.peer_id.as_ref(), TRACKER_ENCODE_SET).to_string(),
    );
    out
}

fn parse_http_peers_list<'a>(
    peer_list: &[Bencode<'a>],
) -> Result<Vec<SocketAddr>> {
    let mut peers = Vec::new();
    for peer in peer_list {
        let ip_raw = peer.get_bytes(b"ip")?;
        let ip_str = std::str::from_utf8(ip_raw).map_err(|_| {
            Error::InvalidTrackerResponse(
                "peer ip was not valid utf-8".to_owned(),
            )
        })?;
        let ip: IpAddr = ip_str.parse().map_err(|_| {
            Error::InvalidTrackerResponse(format!("invalid peer ip: {ip_str}"))
        })?;
        let port_i64 = peer.get_int(b"port")?;
        let port = u16::try_from(port_i64).map_err(|_| {
            Error::InvalidTrackerResponse("peer port out of range".to_owned())
        })?;
        peers.push(SocketAddr::new(ip, port));
    }
    Ok(peers)
}

// --- UDP ---

const UDP_PROTOCOL_ID: u64 = 0x41727101980;
const ACTION_CONNECT: u32 = 0;
const ACTION_ANNOUNCE: u32 = 1;

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
    socket.send(&build_connect_request(tx_id)).await?;

    let mut buf = [0u8; 4096];
    let n = timeout(Duration::from_secs(2), socket.recv(&mut buf))
        .await
        .map_err(|_| Error::Timeout)??;
    let connection_id = parse_connect_response(&buf[..n], tx_id)?;

    let announce_tx = rand::random::<u32>();
    socket
        .send(&build_announce_request(connection_id, announce_tx, req))
        .await?;

    let n = socket.recv(&mut buf).await?;
    parse_announce_response(&buf[..n], announce_tx)
}

fn build_connect_request(tx_id: u32) -> [u8; 16] {
    let mut buf = [0u8; 16];
    buf[0..8].copy_from_slice(&UDP_PROTOCOL_ID.to_be_bytes());
    buf[8..12].copy_from_slice(&ACTION_CONNECT.to_be_bytes());
    buf[12..16].copy_from_slice(&tx_id.to_be_bytes());
    buf
}

fn parse_connect_response(bytes: &[u8], expected_tx_id: u32) -> Result<u64> {
    if bytes.len() < 16 {
        return Err(Error::InvalidTrackerResponse(
            "udp connect response too short".to_owned(),
        ));
    }
    let action = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
    let tx_id = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
    if action != ACTION_CONNECT {
        return Err(Error::InvalidTrackerResponse(format!(
            "unexpected udp connect action: {action}"
        )));
    }
    if tx_id != expected_tx_id {
        return Err(Error::InvalidTrackerResponse(
            "udp connect transaction id mismatch".to_owned(),
        ));
    }
    Ok(u64::from_be_bytes(bytes[8..16].try_into().unwrap()))
}

fn build_announce_request(
    connection_id: u64,
    tx_id: u32,
    req: &AnnounceRequest,
) -> [u8; 98] {
    let mut buf = [0u8; 98];
    buf[0..8].copy_from_slice(&connection_id.to_be_bytes());
    buf[8..12].copy_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
    buf[12..16].copy_from_slice(&tx_id.to_be_bytes());
    buf[16..36].copy_from_slice(req.info_hash.as_ref());
    buf[36..56].copy_from_slice(req.peer_id.as_ref());
    buf[56..64].copy_from_slice(&req.stats.downloaded.to_be_bytes());
    buf[64..72].copy_from_slice(&req.stats.left.to_be_bytes());
    buf[72..80].copy_from_slice(&req.stats.uploaded.to_be_bytes());
    buf[80..84].copy_from_slice(&req.event.as_udp_code().to_be_bytes());
    buf[84..88].copy_from_slice(&0u32.to_be_bytes());
    buf[88..92].copy_from_slice(&rand::random::<u32>().to_be_bytes());
    buf[92..96].copy_from_slice(&(-1i32).to_be_bytes());
    buf[96..98].copy_from_slice(&req.port.to_be_bytes());
    buf
}

fn parse_announce_response(
    bytes: &[u8],
    expected_tx_id: u32,
) -> Result<TrackerResponse> {
    if bytes.len() < 20 {
        return Err(Error::InvalidTrackerResponse(
            "udp announce response too short".to_owned(),
        ));
    }
    let action = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
    let tx_id = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
    if action != ACTION_ANNOUNCE {
        return Err(Error::InvalidTrackerResponse(format!(
            "unexpected udp announce action: {action}"
        )));
    }
    if tx_id != expected_tx_id {
        return Err(Error::InvalidTrackerResponse(
            "udp announce transaction id mismatch".to_owned(),
        ));
    }
    let interval = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
    let leechers = u32::from_be_bytes(bytes[12..16].try_into().unwrap());
    let seeders = u32::from_be_bytes(bytes[16..20].try_into().unwrap());
    let peers = parse_compact_ipv4_peers(&bytes[20..])?;
    Ok(TrackerResponse {
        interval,
        peers,
        seeders: Some(seeders),
        leechers: Some(leechers),
    })
}

// --- utils ---

fn parse_compact_ipv4_peers(bytes: &[u8]) -> Result<Vec<SocketAddr>> {
    if !bytes.len().is_multiple_of(6) {
        return Err(Error::InvalidTrackerResponse(
            "compact ipv4 peers length must be multiple of 6".to_owned(),
        ));
    }
    let mut peers = Vec::with_capacity(bytes.len() / 6);
    for chunk in bytes.chunks_exact(6) {
        let ip =
            IpAddr::V4(Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]));
        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
        peers.push(SocketAddr::new(ip, port));
    }
    Ok(peers)
}
