#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("bencode parsing failed: {0}")]
    Bencode(#[from] crate::bencode::Error),

    #[error("invalid HTTP client")]
    InvalidHttpClient,

    #[error("invalid tracker url")]
    InvalidTrackerUrl,

    #[error("unsupported tracker scheme: {0}")]
    UnsupportedScheme(String),

    #[error("missing udp host")]
    MissingUdpHost,

    #[error("missing udp port")]
    MissingUdpPort,

    #[error("http request failed: {0}")]
    HttpRequest(String),

    #[error("udp request failed: {0}")]
    UdpRequest(String),

    #[error("tracker failure: {0}")]
    TrackerFailure(String),

    #[error("invalid peer id")]
    InvalidPeerId,

    #[error("invalid tracker response: {0}")]
    InvalidTrackerResponse(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
