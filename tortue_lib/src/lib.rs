mod adapters;
mod application;
mod domain;

pub use application::{download::*, errors::Error, metainfo::*};
pub use domain::{
    pool::{PeerInfo, PoolSnapshot},
    torrent::{File, InfoHash, Metainfo, Mode},
};
