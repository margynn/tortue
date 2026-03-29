use std::net::{IpAddr, Ipv4Addr};

use crate::tracker::{Error, PeerAddr};

pub(super) fn parse_compact_ipv4_peers(
    bytes: &[u8],
) -> Result<Vec<PeerAddr>, Error> {
    if bytes.len() % 6 != 0 {
        return Err(Error::InvalidTrackerResponse(
            "compact ipv4 peers length must be multiple of 6".to_owned(),
        ));
    }
    let mut peers = Vec::with_capacity(bytes.len() / 6);

    // first 4 bytes: IP v4 (32 bits), last 2 bytes: port
    for chunk in bytes.chunks_exact(6) {
        let ip =
            IpAddr::V4(Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]));
        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
        peers.push(PeerAddr(ip, port));
    }

    Ok(peers)
}
