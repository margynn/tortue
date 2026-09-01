use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::Instant;

use rand::seq::IteratorRandom;

use super::bitfield::Bitfield;

pub enum Input {
    PeersDiscovered(Vec<SocketAddr>),
    PeerConnected { addr: SocketAddr, bitfield: Bitfield },
    PeerDisconnected(SocketAddr),
    PeerUnchokedUs(SocketAddr),
    PeerChokedUs(SocketAddr),
    PieceAvailable { addr: SocketAddr, index: usize },
    PieceVerified(usize),
    Stop,
}

pub enum Output {
    ConnectPeer(SocketAddr),
    DisconnectPeer(SocketAddr),
    RequestPiece { from: SocketAddr, index: usize },
    Completed, // Should not we specify the completed piece ?
}

pub struct Pool {
    num_pieces: usize,
    needed: Bitfield,
    peers: HashMap<SocketAddr, PeerState>,
    availability: HashMap<usize, HashSet<SocketAddr>>,
    in_flight: HashMap<usize, SocketAddr>,
}

enum PeerState {
    Connecting { since: Instant },
    Connected { unchoked: bool, in_flight: usize },
}

const MAX_IN_FLIGHT_PER_PEER: usize = 30;
const MAX_CONNECTED: usize = 256;

impl Pool {
    pub fn new(num_pieces: usize) -> Self {
        let mut needed = Bitfield::new(num_pieces);
        let _ = needed.set_all();
        Self {
            num_pieces,
            needed,
            peers: HashMap::new(),
            availability: HashMap::new(),
            in_flight: HashMap::new(),
        }
    }

    pub fn step(&mut self, input: Input) -> Vec<Output> {
        match input {
            Input::PeersDiscovered(addrs) => self.on_discovered(addrs),
            Input::PeerConnected { addr, bitfield } => self.on_connected(addr, bitfield),
            Input::PeerDisconnected(addr) => self.on_disconnected(addr),
            Input::PeerUnchokedUs(addr) => self.on_unchoked(addr),
            Input::PeerChokedUs(addr) => self.on_choked(addr),
            Input::PieceAvailable { addr, index } => self.on_piece_available(addr, index),
            Input::PieceVerified(piece) => self.on_piece_verified(piece),
            Input::Stop => self.on_stop(),
        }
    }

    fn on_discovered(&mut self, socket_addrs: Vec<SocketAddr>) -> Vec<Output> {
        let mut output = vec![];
        for addr in socket_addrs {
            if let Entry::Vacant(entry) = self.peers.entry(addr) {
                entry.insert(PeerState::Connecting { since: Instant::now() });
                output.push(Output::ConnectPeer(addr));
            }
        }
        output
    }

    fn on_connected(&mut self, socket_addr: SocketAddr, bitfield: Bitfield) -> Vec<Output> {
        if let Some(state) = self.peers.get_mut(&socket_addr) {
            *state = PeerState::Connected { unchoked: false, in_flight: 0 };
        }
        for piece in &bitfield {
            self.availability.entry(piece as usize).or_default().insert(socket_addr);
        }
        vec![]
    }

    fn on_disconnected(&mut self, addr: SocketAddr) -> Vec<Output> {
        self.peers.remove(&addr);
        self.availability.retain(|_, peers| {
            peers.remove(&addr);
            !peers.is_empty()
        });
        self.in_flight.retain(|_, peer| *peer != addr);
        self.schedule_requests()
    }

    fn on_unchoked(&mut self, addr: SocketAddr) -> Vec<Output> {
        if let Some(PeerState::Connected { unchoked, .. }) = self.peers.get_mut(&addr) {
            *unchoked = true;
        }
        self.schedule_requests()
    }

    fn on_choked(&mut self, addr: SocketAddr) -> Vec<Output> {
        if let Some(PeerState::Connected { unchoked, .. }) = self.peers.get_mut(&addr) {
            *unchoked = false;
        }
        self.in_flight.retain(|_, peer| *peer != addr);
        if let Some(PeerState::Connected { in_flight, .. }) = self.peers.get_mut(&addr) {
            *in_flight = 0;
        }
        self.schedule_requests()
    }

    fn on_piece_available(&mut self, addr: SocketAddr, index: usize) -> Vec<Output> {
        self.availability.entry(index).or_default().insert(addr);
        self.schedule_requests()
    }

    fn on_piece_verified(&mut self, piece: usize) -> Vec<Output> {
        let _ = self.needed.unset_bit(piece);

        if let Some(addr) = self.in_flight.remove(&piece) {
            if let Some(PeerState::Connected { in_flight, .. }) = self.peers.get_mut(&addr) {
                *in_flight = in_flight.saturating_sub(1);
            }
        }

        if (&self.needed).into_iter().next().is_none() {
            return vec![Output::Completed];
        }

        self.schedule_requests()
    }

    fn on_stop(&mut self) -> Vec<Output> {
        self.peers.keys().copied().map(Output::DisconnectPeer).collect()
    }

    fn schedule_requests(&mut self) -> Vec<Output> {
        let mut outputs = vec![];

        let mut needed: Vec<usize> = self.needed.into_iter().map(|i| i as usize).collect();

        needed.sort_by_key(|&piece| {
            self.availability.get(&piece).map_or(usize::MAX, |peers| peers.len())
        });
        let mut rng = rand::rng();

        for piece in needed {
            let Some(peers) = self.availability.get(&piece) else {
                continue;
            };

            let candidate = peers
                .iter()
                .filter(|addr| match self.peers.get(addr) {
                    Some(PeerState::Connected { unchoked: true, in_flight }) => {
                        *in_flight < MAX_IN_FLIGHT_PER_PEER
                    },
                    _ => false,
                })
                .choose(&mut rng);

            if let Some(addr) = candidate {
                if let Some(PeerState::Connected { in_flight, .. }) = self.peers.get_mut(addr) {
                    *in_flight += 1;
                }
                self.in_flight.insert(piece, *addr);
                outputs.push(Output::RequestPiece { from: *addr, index: piece });
            }
        }

        outputs
    }
}
