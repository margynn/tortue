use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

use super::bitfield::Bitfield;
use super::peer::{PeerId, PeerSession};
use super::torrent::Metainfo;

pub enum Input {
    PeersDiscovered(Vec<SocketAddr>),
    PeerConnected {
        addr: SocketAddr,
        peer_id: PeerId,
        bitfield: Bitfield,
    },
    PeerDisconnected(SocketAddr),
    PieceAvailable {
        addr: SocketAddr,
        index: usize,
    },
    PieceDownloaded(usize),
    Stop,
}

pub enum Output {
    ConnectPeer(SocketAddr),
    DisconnectPeer(SocketAddr),
    RequestPiece { from: SocketAddr, index: usize },
    Completed,
}

pub struct PeerPool<'a> {
    metainfo: &'a Metainfo,
    peers: HashMap<SocketAddr, PeerSession>,
    piece_to_peers: HashMap<u32, HashSet<SocketAddr>>,
}

// const MAX_IN_FLIGHT_PER_PEER: usize = 30;
// const PEER_CMD_CHAN_SIZE: usize = 32;
// const SWARM_EVENT_CHAN_SIZE: usize = 256;

impl<'a> PeerPool<'a> {
    pub fn new(metainfo: &'a Metainfo) -> Self {
        Self {
            metainfo,
            peers: HashMap::new(),
            piece_to_peers: HashMap::new(),
        }
    }

    pub fn step(&mut self, input: Input) -> Vec<Output> {
        todo!()
    }
}
