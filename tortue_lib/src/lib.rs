mod adapters;
mod application;
mod domain;

pub use application::{download::*, errors::Error, metainfo::*};
pub use domain::{
    magnet::MagnetLink,
    pool::PoolSnapshot,
    torrent::{File, InfoHash, Metainfo, Mode},
};
