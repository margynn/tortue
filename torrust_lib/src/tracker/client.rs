use reqwest::Client;
use std::time::Duration;

use super::{
    endpoint::TrackerEndpoint,
    error::Error,
    http,
    model::{AnnounceRequest, TrackerResponse},
    udp,
};

#[derive(Debug, Clone)]
pub struct TrackerClient {
    endpoint: TrackerEndpoint,
    http: Client,
}

impl TrackerClient {
    pub fn new(endpoint: TrackerEndpoint) -> Result<Self, Error> {
        let http = Client::builder()
            .timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(8)
            .pool_idle_timeout(Duration::from_secs(90))
            .user_agent("ToRustLib/0.1.0")
            .redirect(reqwest::redirect::Policy::limited(5))
            .build()
            .map_err(|_| Error::InvalidHttpClient)?;
        Ok(Self { endpoint, http })
    }

    pub async fn announce(&self, req: &AnnounceRequest) -> Result<TrackerResponse, Error> {
        match &self.endpoint {
            TrackerEndpoint::Http { url } => http::announce(&self.http, url, req).await,
            TrackerEndpoint::Udp { host, port } => udp::announce(host, *port, req).await,
            TrackerEndpoint::Ws { url } => todo!(),
        }
    }
}
