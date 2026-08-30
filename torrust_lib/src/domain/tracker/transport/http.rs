use std::net::{IpAddr, SocketAddr};

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_encode};
use url::Url;

use super::super::{AnnounceRequest, TrackerResponse};
use super::{Error, Result};
use crate::domain::bencode::Bencode;

const ENCODE_SET: &AsciiSet = NON_ALPHANUMERIC;

pub fn build_announce_url(base_url: &Url, req: &AnnounceRequest) -> String {
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
        &percent_encode(req.info_hash.as_ref(), ENCODE_SET).to_string(),
    );
    out.push_str("&peer_id=");
    out.push_str(&percent_encode(req.peer_id.as_ref(), ENCODE_SET).to_string());
    out
}

pub fn parse_response(bytes: &[u8]) -> Result<TrackerResponse> {
    let decoded = Bencode::decode(bytes)?;

    if let Ok(reason) = decoded.get_bytes(b"failure reason") {
        return Err(Error::TrackerFailure(
            String::from_utf8_lossy(reason).into_owned(),
        ));
    }

    let interval_i64 = decoded.get_int(b"interval")?;
    let interval = u32::try_from(interval_i64).map_err(|_| {
        Error::InvalidResponse("interval out of range".to_owned())
    })?;

    let peers = match (decoded.get_bytes(b"peers"), decoded.get_list(b"peers"))
    {
        (Ok(compact), _) => super::parse_compact_ipv4_peers(compact)?,
        (_, Ok(list)) => parse_peers_list(list)?,
        _ => {
            return Err(Error::InvalidResponse(
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

fn parse_peers_list(peer_list: &[Bencode<'_>]) -> Result<Vec<SocketAddr>> {
    let mut peers = Vec::new();
    for peer in peer_list {
        let ip_raw = peer.get_bytes(b"ip")?;
        let ip_str = std::str::from_utf8(ip_raw).map_err(|_| {
            Error::InvalidResponse("peer ip was not valid utf-8".to_owned())
        })?;
        let ip: IpAddr = ip_str.parse().map_err(|_| {
            Error::InvalidResponse(format!("invalid peer ip: {ip_str}"))
        })?;
        let port_i64 = peer.get_int(b"port")?;
        let port = u16::try_from(port_i64).map_err(|_| {
            Error::InvalidResponse("peer port out of range".to_owned())
        })?;
        peers.push(SocketAddr::new(ip, port));
    }
    Ok(peers)
}
