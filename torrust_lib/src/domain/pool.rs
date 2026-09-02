use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

use rand::seq::IteratorRandom;

use super::bitfield::Bitfield;
use super::peer::{Message, PeerId};
use super::torrent::Metainfo;

pub enum Input {
    PeersDiscovered(Vec<SocketAddr>),
    PeerConnected { addr: SocketAddr, peer_id: PeerId },
    PeerDisconnected(SocketAddr),
    MessageReceived { addr: SocketAddr, message: Message },
    PieceVerified(usize),
}

pub enum Output {
    ConnectPeer(SocketAddr),
    SendToPeer { addr: SocketAddr, message: Message },
    Completed,
}

pub struct Pool {
    metainfo: Metainfo,
    needed: Bitfield,
    peers: HashMap<SocketAddr, PeerState>,
    availability: HashMap<usize, HashSet<SocketAddr>>, // piece -> peers
    in_flight: HashMap<(usize, u32), SocketAddr>,      // (piece, begin) -> peer
}

const MAX_IN_FLIGHT_PER_PEER: usize = 30;
const MAX_CONNECTED: usize = 256;
const BLOCK_SIZE: u32 = 16_384; // 16KB

impl Pool {
    pub fn new(metainfo: Metainfo) -> Self {
        let mut needed = Bitfield::new(metainfo.pieces.len());
        let _ = needed.set_all();
        Self {
            metainfo,
            needed,
            peers: HashMap::new(),
            availability: HashMap::new(),
            in_flight: HashMap::new(),
        }
    }

    pub fn step(&mut self, input: Input) -> Vec<Output> {
        match input {
            Input::PeersDiscovered(addrs) => self.on_discovered(addrs),
            Input::PeerConnected { addr, .. } => self.on_connected(addr),
            Input::PeerDisconnected(addr) => self.on_disconnected(addr),
            Input::MessageReceived { addr, message } => self.on_message(addr, message),
            Input::PieceVerified(piece) => self.on_piece_verified(piece),
        }
    }

    fn on_discovered(&mut self, socket_addrs: Vec<SocketAddr>) -> Vec<Output> {
        let mut output = vec![];
        for addr in socket_addrs {
            if let Entry::Vacant(entry) = self.peers.entry(addr) {
                entry.insert(PeerState::new(self.metainfo.pieces.len()));
                output.push(Output::ConnectPeer(addr));
            }
        }
        output
    }

    fn on_connected(&mut self, addr: SocketAddr) -> Vec<Output> {
        // Also handles future inbound connections not preceded by PeersDiscovered.
        self.peers
            .entry(addr)
            .or_insert_with(|| PeerState::new(self.metainfo.pieces.len()));
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

    fn on_message(&mut self, addr: SocketAddr, message: Message) -> Vec<Output> {
        if let Some(state) = self.peers.get_mut(&addr) {
            state.apply(&message);
        } else {
            return vec![];
        }

        match &message {
            Message::Bitfield(bits) => {
                if let Ok(bf) = Bitfield::try_from(bits.as_ref()) {
                    for piece in &bf {
                        self.availability.entry(piece as usize).or_default().insert(addr);
                    }
                }
                let mut outputs = vec![Output::SendToPeer { addr, message: Message::Interested }];
                outputs.extend(self.schedule_requests());
                outputs
            },
            Message::Have(piece) => {
                self.availability.entry(*piece as usize).or_default().insert(addr);
                self.schedule_requests()
            },
            Message::Unchoke => self.schedule_requests(),
            Message::Choke => {
                self.in_flight.retain(|_, peer| *peer != addr);
                if let Some(state) = self.peers.get_mut(&addr) {
                    state.in_flight = 0;
                }
                self.schedule_requests()
            },
            Message::Piece { index, begin, .. } => {
                self.on_block_received(addr, *index as usize, *begin)
            },
            _ => vec![],
        }
    }

    fn on_block_received(&mut self, addr: SocketAddr, piece: usize, begin: u32) -> Vec<Output> {
        if self.in_flight.remove(&(piece, begin)).is_some() {
            if let Some(state) = self.peers.get_mut(&addr) {
                state.in_flight = state.in_flight.saturating_sub(1);
            }
        }
        self.schedule_requests()
    }

    fn on_piece_verified(&mut self, piece: usize) -> Vec<Output> {
        let _ = self.needed.unset_bit(piece);

        if (&self.needed).into_iter().next().is_none() {
            return vec![Output::Completed];
        }

        self.schedule_requests()
    }

    fn schedule_requests(&mut self) -> Vec<Output> {
        let mut outputs = vec![];

        // Rarest first
        let mut needed: Vec<usize> = self.needed.into_iter().map(|i| i as usize).collect();
        needed.sort_by_key(|&piece| {
            self.availability.get(&piece).map_or(usize::MAX, |peers| peers.len())
        });

        let mut rng = rand::rng();

        for piece in needed {
            // Collect owned addrs — releases the borrow on self.availability before
            // the inner loop mutates self.peers.
            let peer_addrs: Vec<SocketAddr> = match self.availability.get(&piece) {
                Some(peers) => peers.iter().copied().collect(),
                None => continue,
            };

            for (begin, length) in self.piece_blocks(piece) {
                if self.in_flight.contains_key(&(piece, begin)) {
                    continue;
                }

                let candidate = peer_addrs
                    .iter()
                    .filter(|addr| {
                        self.peers.get(addr).map_or(false, |s| {
                            !s.peer_choking && s.in_flight < MAX_IN_FLIGHT_PER_PEER
                        })
                    })
                    .choose(&mut rng)
                    .copied();

                if let Some(addr) = candidate {
                    self.peers.get_mut(&addr).unwrap().in_flight += 1;
                    self.in_flight.insert((piece, begin), addr);

                    outputs.push(Output::SendToPeer {
                        addr,
                        message: Message::Request { index: piece as u32, begin, length },
                    });
                }
            }
        }

        outputs
    }

    fn piece_blocks(&self, piece: usize) -> Vec<(u32, u32)> {
        // (begin, length) for every block of a piece
        let piece_len = if piece == self.metainfo.pieces.len() - 1 {
            let total = self.metainfo.size();
            let full = (self.metainfo.pieces.len() - 1) as u64 * self.metainfo.piece_length;
            (total - full) as u32
        } else {
            self.metainfo.piece_length as u32
        };

        (0..)
            .map(|i| i * BLOCK_SIZE)
            .take_while(|&begin| begin < piece_len)
            .map(|begin| (begin, BLOCK_SIZE.min(piece_len - begin)))
            .collect()
    }
}

struct PeerState {
    am_choking: bool,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
    bitfield: Bitfield,
    in_flight: usize,
}

impl PeerState {
    fn new(pieces: usize) -> Self {
        Self {
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            bitfield: Bitfield::new(pieces),
            in_flight: 0,
        }
    }

    fn apply(&mut self, msg: &Message) {
        match msg {
            Message::Choke => self.peer_choking = true,
            Message::Unchoke => self.peer_choking = false,
            Message::Interested => self.peer_interested = true,
            Message::NotInterested => self.peer_interested = false,
            Message::Bitfield(bits) => {
                if let Ok(bf) = Bitfield::try_from(bits.as_ref()) {
                    self.bitfield = bf;
                }
            },
            Message::Have(piece) => {
                let _ = self.bitfield.set_bit(*piece as usize);
            },
            _ => {},
        }
    }
}
