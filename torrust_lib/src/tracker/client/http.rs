use std::net::IpAddr;

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_encode};
use reqwest::Client;
use url::Url;

use crate::bencode::{Bencode, decode};
use crate::tracker::{AnnounceRequest, Error, PeerAddr, TrackerResponse};

const TRACKER_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC;

pub(super) async fn announce(
    client: &Client,
    base_url: &Url,
    req: &AnnounceRequest,
) -> Result<TrackerResponse, Error> {
    let url = build_announce_url(base_url, req);
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| Error::HttpRequest(e.to_string()))?;
    let bytes = response
        .bytes()
        .await
        .map_err(|e| Error::HttpRequest(e.to_string()))?;
    let decoded = decode(&bytes)?;

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
        (Ok(compact), _) => super::utils::parse_compact_ipv4_peers(compact)?,
        (_, Ok(list)) => parse_peers_list(list)?,
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
    }

    let sep = if url.query().is_some() { "&" } else { "?" };
    let mut out: String = url.into();
    out.push_str(sep);
    out.push_str("info_hash=");
    out.push_str(
        &percent_encode(&req.info_hash, TRACKER_ENCODE_SET).to_string(),
    );
    out.push_str("&peer_id=");
    out.push_str(
        &percent_encode(req.peer_id.as_ref(), TRACKER_ENCODE_SET).to_string(),
    );
    out
}

fn parse_peers_list<'a>(
    peer_list: &[Bencode<'a>],
) -> Result<Vec<PeerAddr>, Error> {
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

        peers.push(PeerAddr(ip, port));
    }
    Ok(peers)
}
