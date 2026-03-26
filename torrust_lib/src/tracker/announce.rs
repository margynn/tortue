use crate::bencode::decode;
use crate::metainfo::Metainfo;
use crate::tracker::{Error, Peer, PeerId, Tracker, TrackerResponse};

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_encode};
use std::net::IpAddr;
use url::Url;

const TRACKER_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC;

impl Tracker {
    pub async fn announce(&self, metainfo: &Metainfo) -> Result<TrackerResponse, Error> {
        let url = self.build_announce_url(metainfo)?;

        println!("Announce URL: {}", url);

        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|_| Error::RequestFailed)?
            .bytes()
            .await
            .map_err(|_| Error::RequestFailed)?;
        let decoded = decode(&response)?;

        if let Ok(reason) = decoded.get_bytes(b"failure reason") {
            return Err(Error::TrackerFailure(String::from_utf8_lossy(reason).to_string()));
        }

        let interval = decoded.get_int(b"interval")? as u64;
        let mut peers = Vec::new();
        for peer in decoded.get_list(b"peers")? {
            let peer_id = match <[u8; 20]>::try_from(peer.get_bytes(b"peer id")?) {
                Ok(arr) => PeerId::new(arr),
                Err(_) => return Err(Error::InvalidPeerId), // your custom error
            };
            let ip_bytes = peer.get_bytes(b"ip")?;
            let port_bytes = peer.get_bytes(b"port")?;

            let ip = match ip_bytes.len() {
                4 => IpAddr::from(<[u8; 4]>::try_from(ip_bytes).unwrap()),
                16 => IpAddr::from(<[u8; 16]>::try_from(ip_bytes).unwrap()),
                _ => {
                    return Err(Error::TrackerFailure(format!(
                        "invalid IP length: {}",
                        ip_bytes.len()
                    )));
                },
            };

            // Convert port (big-endian)
            let port = u16::from_be_bytes(<[u8; 2]>::try_from(port_bytes).unwrap());
            peers.push(Peer { peer_id, ip, port });
        }

        Ok(TrackerResponse { interval, peers })
    }

    pub fn build_announce_url(&self, metainfo: &Metainfo) -> Result<Url, Error> {
        let info_hash = percent_encode(metainfo.hash.as_ref(), TRACKER_ENCODE_SET).to_string();
        let peer_id = percent_encode(self.peer_id.as_ref(), TRACKER_ENCODE_SET).to_string();

        let mut url = Url::parse(&self.announce_url).map_err(|_| Error::InvalidAnnounceURL)?;

        {
            let mut qp = url.query_pairs_mut();
            qp.append_pair("port", &self.port.to_string());
            qp.append_pair("uploaded", "0");
            qp.append_pair("downloaded", "0");
            qp.append_pair("left", "0"); // TODO: use real value
            qp.append_pair("compact", "1");
            qp.append_pair("event", "started");
        }

        let final_url = format!("{}&info_hash={}&peer_id={}", url.as_str(), info_hash, peer_id);

        Url::parse(&final_url).map_err(|_| Error::InvalidAnnounceURL)
    }
}
