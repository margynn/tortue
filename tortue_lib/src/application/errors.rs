#[derive(Debug, thiserror::Error)]
pub enum DownloadError {
    #[error("invalid torrent file: {0}")]
    InvalidTorrentFile(String),

    #[error("storage error: {0}")]
    Storage(#[from] std::io::Error),

    #[error("download failed: {0}")]
    Failed(String),
}
