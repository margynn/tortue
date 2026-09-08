use crate::adapters::metadata_io::Error as MetadataError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid torrent file: {0}")]
    InvalidTorrentFile(String),

    #[error("invalid metainfo")]
    InvalidMetainfo,

    #[error("download failed: {0}")]
    Failed(String),

    #[error("metadata fetch failed: {0}")]
    MetadataFetch(#[from] MetadataError),
}
pub type Result<T> = std::result::Result<T, Error>;
