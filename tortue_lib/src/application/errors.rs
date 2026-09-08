#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid torrent file: {0}")]
    InvalidTorrentFile(String),

    #[error("invalid metainfo")]
    InvalidMetainfo,

    #[error("storage error: {0}")]
    Storage(#[from] std::io::Error),

    #[error("download failed: {0}")]
    Failed(String),
}
pub type Result<T> = std::result::Result<T, Error>;
