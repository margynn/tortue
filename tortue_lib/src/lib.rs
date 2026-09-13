mod adapters;
mod application;
mod domain;

pub use application::{download::*, errors::Error, metainfo::*};
pub use domain::{
    coordinator::CoordinatorSnapshot,
    magnet::MagnetLink,
    torrent::{File, InfoHash, Metainfo, Mode},
};
