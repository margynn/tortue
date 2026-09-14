mod adapters;
mod application;
mod domain;

pub use application::{download::*, errors::Error, metainfo::*};
pub use domain::{
    magnet::MagnetLink,
    swarm::SwarmSnapshot,
    torrent::{File, InfoHash, Metainfo, Mode},
};
