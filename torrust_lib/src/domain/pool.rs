use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::vec;

use rand::seq::IteratorRandom;

use super::bitfield::Bitfield;
use super::peer::{Message, PeerId};
use super::pieces::{PieceEvent, PieceManager};
use super::torrent::Metainfo;
use crate::domain::pieces::BlockRef;

pub enum Input {
    PeersDiscovered(Vec<SocketAddr>),
    PeerConnected { addr: SocketAddr, peer_id: PeerId },
    PeerDisconnected(SocketAddr),
    MessageReceived { addr: SocketAddr, message: Message },
}

pub enum Output {
    ConnectPeer(SocketAddr),
    SendToPeer {
        addr: SocketAddr,
        message: Message,
    },
    WritePiece {
        piece_index: usize,
        piece_offset: u64,
        data: Vec<u8>,
    },
    Completed,
}

type PieceIndex = usize;

pub struct Pool<'a> {
    metainfo: &'a Metainfo,
    peers: HashMap<SocketAddr, PeerState>,
    availability: HashMap<PieceIndex, HashSet<SocketAddr>>,
    block_assignments: HashMap<BlockRef, SocketAddr>,
    pieces: PieceManager<'a>,
}

const MAX_IN_FLIGHT_PER_PEER: usize = 30;
const MAX_CONNECTED: usize = 256;

impl<'a> Pool<'a> {
    pub fn new(metainfo: &'a Metainfo) -> Self {
        let pieces = PieceManager::new(metainfo);
        Self {
            metainfo,
            peers: HashMap::new(),
            availability: HashMap::new(),
            block_assignments: HashMap::new(),
            pieces,
        }
    }

    pub fn step(&mut self, input: Input) -> Vec<Output> {
        match input {
            Input::PeersDiscovered(addrs) => self.on_discovered(addrs),
            Input::PeerConnected { addr, .. } => self.on_connected(addr),
            Input::PeerDisconnected(addr) => self.on_disconnected(addr),
            Input::MessageReceived { addr, message } => self.on_message(addr, message),
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

        // Reset orphaned in-flight blocks in PieceManager before removing them.
        let orphaned: Vec<BlockRef> = self
            .block_assignments
            .iter()
            .filter(|(_, peer)| **peer == addr)
            .map(|(key, _)| *key)
            .collect();
        for block_ref in orphaned {
            self.block_assignments.remove(&block_ref);
            self.pieces.request_block(block_ref);
        }

        self.schedule_requests()
    }

    fn on_message(&mut self, addr: SocketAddr, message: Message) -> Vec<Output> {
        let Some(state) = self.peers.get_mut(&addr) else {
            return vec![];
        };
        state.apply(&message);
        match message {
            Message::Bitfield(bits) => self.on_message_bitfield(addr, bits),
            Message::Have(piece_index) => self.on_message_have(addr, piece_index),
            Message::Unchoke => self.schedule_requests(),
            Message::Choke => self.on_message_choke(addr),
            Message::Piece { piece_index, piece_offset, data } => {
                let block_ref = BlockRef { piece_index, piece_offset };
                self.on_message_piece(addr, block_ref, data)
            },
            _ => vec![],
        }
    }

    /// Ask the peer to unchoke us (Send Interrested), or send block requests if
    /// already unchocked
    fn interested_or_request(&mut self, addr: SocketAddr) -> Vec<Output> {
        let peer = self.peers.get_mut(&addr).expect("peer must be available");
        if !peer.am_interested {
            peer.am_interested = true;
            // Signal interest unconditionally — peer will unchoke us if they agree.
            return vec![Output::SendToPeer { addr, message: Message::Interested }];
        }
        if peer.peer_choking {
            return vec![]; // Already interested, waiting for unchoke.
        }
        self.schedule_requests()
    }

    fn on_message_bitfield(&mut self, addr: SocketAddr, bits: Vec<u8>) -> Vec<Output> {
        // Record the pieces available at peer
        if let Ok(bf) = Bitfield::try_from(bits.as_ref()) {
            for piece in &bf {
                self.availability.entry(piece).or_default().insert(addr);
            }
        }
        self.interested_or_request(addr)
    }

    fn on_message_have(&mut self, addr: SocketAddr, piece_index: usize) -> Vec<Output> {
        // Update the piece availability at peer
        self.availability.entry(piece_index).or_default().insert(addr);
        self.interested_or_request(addr)
    }

    fn on_message_choke(&mut self, addr: SocketAddr) -> Vec<Output> {
        self.block_assignments.retain(|_, peer| *peer != addr);
        if let Some(state) = self.peers.get_mut(&addr) {
            state.in_flight = 0;
        }
        vec![]
    }

    fn on_message_piece(
        &mut self,
        addr: SocketAddr,
        block_ref: BlockRef,
        data: Vec<u8>,
    ) -> Vec<Output> {
        // Free in_flight peer slot
        if let None = self.block_assignments.remove(&block_ref) {
            // block not requested - assume malicious or unwanted
            return vec![];
        }
        if let Some(state) = self.peers.get_mut(&addr) {
            state.in_flight = state.in_flight.saturating_sub(1);
        }

        // Piece management
        match self.pieces.receive_block(block_ref, data) {
            Err(_) => vec![], // malformed block - drop silently
            Ok(piece_event) => match piece_event {
                PieceEvent::BlockReceived => self.schedule_requests(),
                PieceEvent::PieceInvalid { .. } => self.schedule_requests(),
                PieceEvent::PieceCompleted { piece_index, piece_offset, data } => {
                    let write = Output::WritePiece { piece_index, piece_offset, data };
                    if self.pieces.is_complete() {
                        return vec![write, Output::Completed];
                    }
                    let mut outputs = self.schedule_requests();
                    outputs.push(write);
                    outputs
                },
            },
        }
    }

    fn schedule_requests(&mut self) -> Vec<Output> {
        let mut outputs = vec![];

        // Rarest first
        let mut needed: Vec<usize> = self.pieces.needed_pieces().collect();
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

    // TODO: replace with piece_manager
    // fn piece_blocks(&self, piece: usize) -> Vec<(u32, u32)> {
    //     // (begin, length) for every block of a piece
    //     let piece_len = if piece == self.metainfo.pieces.len() - 1 {
    //         let total = self.metainfo.size();
    //         let full = (self.metainfo.pieces.len() - 1) as u64 * self.metainfo.piece_length;
    //         (total - full) as u32
    //     } else {
    //         self.metainfo.piece_length as u32
    //     };

    //     (0..)
    //         .map(|i| i * BLOCK_SIZE)
    //         .take_while(|&begin| begin < piece_len)
    //         .map(|begin| (begin, BLOCK_SIZE.min(piece_len - begin)))
    //         .collect()
    // }
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
            // Message::KeepAlive => todo!(),
            // Message::Request { piece_index, piece_offset, piece_len } => todo!(),
            // Message::Piece { piece_index, piece_offset, data } => todo!(),
            // Message::Cancel { piece_index, piece_offset, piece_len } => todo!(),
        }
    }
}
