mod adapters;
mod application;
mod domain;

pub use application::{
    download::{Download, download},
    errors::DownloadError,
    metainfo::metainfo,
};
pub use domain::{
    pool::{PeerInfo, PoolSnapshot},
    torrent::{File, InfoHash, Metainfo, Mode},
};
