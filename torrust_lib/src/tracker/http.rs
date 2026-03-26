use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_encode};
use reqwest::Client;
use url::Url;

use crate::bencode::decode;

use super::{
    PeerId,
    error::Error,
    model::{AnnounceRequest, Peer, TrackerResponse},
};

const TRACKER_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC;

pub async fn announce(
    client: &Client,
    base_url: &Url,
    req: &AnnounceRequest,
) -> Result<TrackerResponse, Error> {
    let url = build_announce_url(base_url, req);

    let response = client.get(url).send().await.map_err(|e| Error::HttpRequest(e.to_string()))?;

    let bytes = response.bytes().await.map_err(|e| Error::HttpRequest(e.to_string()))?;

    let decoded = decode(&bytes)?;

    if let Ok(reason) = decoded.get_bytes(b"failure reason") {
        return Err(Error::TrackerFailure(String::from_utf8_lossy(reason).into_owned()));
    }

    let interval_i64 = decoded.get_int(b"interval")?;
    let interval = u32::try_from(interval_i64)
        .map_err(|_| Error::InvalidTrackerResponse("interval out of range".to_owned()))?;

    let mut peers = Vec::new();

    if let Ok(compact) = decoded.get_bytes(b"peers") {
        peers.extend(parse_compact_ipv4_peers(compact)?);
    } else if let Ok(peer_list) = decoded.get_list(b"peers") {
        for peer in peer_list {
            let ip_raw = peer.get_bytes(b"ip")?;
            let ip_str = std::str::from_utf8(ip_raw).map_err(|_| {
                Error::InvalidTrackerResponse("peer ip was not valid utf-8".to_owned())
            })?;
            let ip: IpAddr = ip_str
                .parse()
                .map_err(|_| Error::InvalidTrackerResponse(format!("invalid peer ip: {ip_str}")))?;

            let port_i64 = peer.get_int(b"port")?;
            let port = u16::try_from(port_i64)
                .map_err(|_| Error::InvalidTrackerResponse("peer port out of range".to_owned()))?;

            let peer_id = match peer.get_bytes(b"peer id") {
                Ok(raw) => {
                    let bytes: [u8; 20] = raw.try_into().map_err(|_| Error::InvalidPeerId)?;
                    Some(PeerId::new(bytes))
                },
                Err(_) => None,
            };

            peers.push(Peer { peer_id, ip, port });
        }
    } else {
        return Err(Error::InvalidTrackerResponse("missing peers field".to_owned()));
    }

    if let Ok(compact6) = decoded.get_bytes(b"peers6") {
        peers.extend(parse_compact_ipv6_peers(compact6)?);
    }

    Ok(TrackerResponse { interval, peers })
}

fn build_announce_url(base_url: &Url, req: &AnnounceRequest) -> String {
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

        if let Some(numwant) = req.numwant {
            qp.append_pair("numwant", &numwant.to_string());
        }
    }

    let sep = if url.query().is_some() { "&" } else { "?" };
    let mut out: String = url.into();
    out.push_str(sep);
    out.push_str("info_hash=");
    out.push_str(&percent_encode(&req.info_hash, TRACKER_ENCODE_SET).to_string());
    out.push_str("&peer_id=");
    out.push_str(&percent_encode(req.peer_id.as_ref(), TRACKER_ENCODE_SET).to_string());
    out
}

fn parse_compact_ipv4_peers(bytes: &[u8]) -> Result<Vec<Peer>, Error> {
    if bytes.len() % 6 != 0 {
        return Err(Error::InvalidTrackerResponse(
            "compact ipv4 peers length must be multiple of 6".to_owned(),
        ));
    }

    let mut peers = Vec::with_capacity(bytes.len() / 6);

    for chunk in bytes.chunks_exact(6) {
        let ip = IpAddr::V4(Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]));
        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
        peers.push(Peer { peer_id: None, ip, port });
    }

    Ok(peers)
}

fn parse_compact_ipv6_peers(bytes: &[u8]) -> Result<Vec<Peer>, Error> {
    if bytes.len() % 18 != 0 {
        return Err(Error::InvalidTrackerResponse(
            "compact ipv6 peers length must be multiple of 18".to_owned(),
        ));
    }

    let mut peers = Vec::with_capacity(bytes.len() / 18);

    for chunk in bytes.chunks_exact(18) {
        let mut ip = [0u8; 16];
        ip.copy_from_slice(&chunk[..16]);
        let ip = IpAddr::V6(Ipv6Addr::from(ip));
        let port = u16::from_be_bytes([chunk[16], chunk[17]]);
        peers.push(Peer { peer_id: None, ip, port });
    }

    Ok(peers)
}
