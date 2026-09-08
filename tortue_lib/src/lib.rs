mod adapters;
mod application;
mod domain;

pub use application::errors::Error;
pub use domain::{
    pool::{PeerInfo, PoolSnapshot},
    torrent::{File, InfoHash, Metainfo, Mode},
};
