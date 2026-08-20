use std::net::SocketAddr;

use tokio::net::{UdpSocket, lookup_host};
use tokio::time::{Duration, timeout};

use crate::tracker::{AnnounceRequest, Error, TrackerResponse};

const UDP_PROTOCOL_ID: u64 = 0x41727101980;
const ACTION_CONNECT: u32 = 0;
const ACTION_ANNOUNCE: u32 = 1;

pub(super) async fn announce(
    host: &str,
    port: u16,
    req: &AnnounceRequest,
) -> Result<TrackerResponse, Error> {
    let tracker_addr = resolve_udp_tracker(host, port).await?;
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(tracker_addr).await?;

    let tx_id = rand::random::<u32>();
    let connect_req = build_connect_request(tx_id);
    socket.send(&connect_req).await?;

    let mut buf = [0u8; 4096];
    let n = timeout(Duration::from_secs(2), socket.recv(&mut buf))
        .await
        .map_err(|_| Error::Timeout("tracker connect timeout".into()))??;
    let connection_id = parse_connect_response(&buf[..n], tx_id)?;

    let announce_tx = rand::random::<u32>();
    let announce_req = build_announce_request(connection_id, announce_tx, req);
    socket.send(&announce_req).await?;

    let n = socket.recv(&mut buf).await?;
    parse_announce_response(&buf[..n], announce_tx)
}

async fn resolve_udp_tracker(
    host: &str,
    port: u16,
) -> Result<SocketAddr, Error> {
    let mut addrs = lookup_host((host, port))
        .await
        .map_err(|e| Error::UdpRequest(e.to_string()))?;

    addrs.next().ok_or_else(|| {
        Error::UdpRequest("tracker hostname resolved to no address".to_owned())
    })
}

fn build_connect_request(tx_id: u32) -> [u8; 16] {
    let mut buf = [0u8; 16];
    buf[0..8].copy_from_slice(&UDP_PROTOCOL_ID.to_be_bytes());
    buf[8..12].copy_from_slice(&ACTION_CONNECT.to_be_bytes());
    buf[12..16].copy_from_slice(&tx_id.to_be_bytes());
    buf
}

fn parse_connect_response(
    bytes: &[u8],
    expected_tx_id: u32,
) -> Result<u64, Error> {
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

    let connection_id = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
    Ok(connection_id)
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
    // ip address = default (0)
    buf[84..88].copy_from_slice(&0u32.to_be_bytes());
    // random key
    let key = rand::random::<u32>();
    buf[88..92].copy_from_slice(&key.to_be_bytes());
    // numwant = default (-1)
    buf[92..96].copy_from_slice(&(-1i32).to_be_bytes());
    buf[96..98].copy_from_slice(&req.port.to_be_bytes());
    buf
}

fn parse_announce_response(
    bytes: &[u8],
    expected_tx_id: u32,
) -> Result<TrackerResponse, Error> {
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
    let peers = super::utils::parse_compact_ipv4_peers(&bytes[20..])?;

    Ok(TrackerResponse {
        interval,
        peers,
        seeders: Some(seeders),
        leechers: Some(leechers),
    })
}
