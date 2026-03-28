mod http;
mod udp;
mod utils;

use std::time::Duration;

use reqwest::Client;
use url::Url;

use super::{AnnounceRequest, Error, TrackerResponse};

#[derive(Debug, Clone)]
enum TrackerEndpoint {
    Udp { host: String, port: u16 },
    Http { url: Url },
    Ws { url: Url },
}

impl TrackerEndpoint {
    fn new(s: &str) -> Result<Self, Error> {
        let url = Url::parse(s).map_err(|_| Error::InvalidTrackerUrl)?;
        match url.scheme() {
            "http" | "https" => Ok(Self::Http { url }),
            "udp" => {
                let host =
                    url.host_str().ok_or(Error::MissingUdpHost)?.to_owned();
                let port = url.port().ok_or(Error::MissingUdpPort)?;
                Ok(Self::Udp { host, port })
            },
            "ws" | "wss" => unimplemented!("websocket not yet implemented"),
            other => Err(Error::UnsupportedScheme(other.to_owned())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TrackerClient {
    endpoint: TrackerEndpoint,
    http: Client,
}

impl TrackerClient {
    pub fn new(s: &str) -> Result<Self, Error> {
        let endpoint = TrackerEndpoint::new(s)?;
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

    pub async fn announce(
        &self,
        req: &AnnounceRequest,
    ) -> Result<TrackerResponse, Error> {
        match &self.endpoint {
            TrackerEndpoint::Http { url } => {
                http::announce(&self.http, url, req).await
            },
            TrackerEndpoint::Udp { host, port } => {
                udp::announce(host, *port, req).await
            },
            TrackerEndpoint::Ws { url } => todo!(),
        }
    }
}
