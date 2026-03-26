mod announce;
mod peer_id;

pub use peer_id::PeerId;
use reqwest::Client;
use std::{net::IpAddr, time::Duration};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("bencode parsing failed: {0}")]
    Bencode(#[from] crate::bencode::Error),
    #[error("invalid HTTP client")]
    InvalidHTTPClient,
    #[error("invalid announce URL")]
    InvalidAnnounceURL,
    #[error("request failed")]
    RequestFailed,
    #[error("tracker failure: {0}")]
    TrackerFailure(String),
    #[error("invalid peer id")]
    InvalidPeerId,
}

#[derive(Debug, Clone)]
pub struct Tracker {
    announce_url: String,
    peer_id: PeerId,
    port: u16,
    uploaded: u64,
    downloaded: u64,
    left: u64,
    client: Client,
}

#[derive(Debug, Clone)]
pub struct Peer {
    pub peer_id: PeerId,
    pub ip: IpAddr,
    pub port: u16,
}

#[derive(Debug, Clone)]
pub struct TrackerResponse {
    pub interval: u64,
    pub peers: Vec<Peer>,
}

impl Tracker {
    /// Build a new tracker instance with a pre-configured HTTP client
    pub fn new(announce_url: String, peer_id: PeerId, port: u16) -> Result<Self, Error> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(300))
            .user_agent("ToRustLib/0.1.0")
            .redirect(reqwest::redirect::Policy::limited(5))
            .build()
            .map_err(|_| Error::InvalidHTTPClient)?;
        Ok(Tracker { announce_url, peer_id, port, uploaded: 0, downloaded: 0, left: 0, client })
    }
}
