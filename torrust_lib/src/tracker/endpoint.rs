use url::Url;

use super::Error;

#[derive(Debug, Clone)]
pub enum TrackerEndpoint {
    Http(Url),
    Udp { host: String, port: u16 },
}

impl TrackerEndpoint {
    pub fn parse(s: &str) -> Result<Self, Error> {
        let url = Url::parse(s).map_err(|_| Error::InvalidTrackerUrl)?;

        match url.scheme() {
            "http" | "https" => Ok(Self::Http(url)),
            "udp" => {
                let host = url.host_str().ok_or(Error::MissingUdpHost)?.to_owned();
                let port = url.port().ok_or(Error::MissingUdpPort)?;
                Ok(Self::Udp { host, port })
            },
            other => Err(Error::UnsupportedScheme(other.to_owned())),
        }
    }
}
