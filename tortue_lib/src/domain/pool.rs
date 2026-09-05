use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    net::SocketAddr,
    sync::Arc,
    vec,
};

use rand::seq::IteratorRandom;

use super::{
    bitfield::Bitfield,
    message::Message,
    peer::PeerId,
    pieces::{BlockRange, BlockRef, PieceEvent, PieceManager},
    torrent::Metainfo,
};

pub enum Input {
    PeersDiscovered(Vec<SocketAddr>),
    PeerConnected { addr: SocketAddr, peer_id: PeerId },
    PeerDisconnected(SocketAddr),
    MessageReceived { addr: SocketAddr, message: Message },
    Tick,
}

pub enum Output {
    ConnectPeer(SocketAddr),
    SendToPeer { addr: SocketAddr, message: Message },
    WritePiece { offset: u64, data: Vec<u8> },
    Broadcast(Message),
    Completed,
}

type PieceIndex = usize;

pub struct Pool {
    metainfo: Arc<Metainfo>,
    peers: HashMap<SocketAddr, PeerState>,
    availability: HashMap<PieceIndex, HashSet<SocketAddr>>,
    block_assignments: HashMap<BlockRef, SocketAddr>,
    pieces: PieceManager,
}

pub struct PoolSnapshot {
    pub blocks_total: usize,
    pub blocks_done: usize,
    pub blocks_in_flight: usize,
    pub peers: Vec<PeerInfo>,
}

pub struct PeerInfo {
    pub addr: SocketAddr,
    pub peer_id: Option<PeerId>,
    pub in_flight: usize,
    pub is_choking: bool,
}

impl Pool {
    pub fn new(metainfo: Arc<Metainfo>) -> Self {
        let pieces = PieceManager::new(Arc::clone(&metainfo));
        Self {
            metainfo,
            peers: HashMap::new(),
            availability: HashMap::new(),
            block_assignments: HashMap::new(),
            pieces,
        }
    }

    pub fn snapshot(&self) -> PoolSnapshot {
        let peers = self
            .peers
            .iter()
            .map(|(addr, s)| PeerInfo {
                addr: *addr,
                peer_id: s.peer_id,
                in_flight: s.in_flight,
                is_choking: s.peer_choking,
            })
            .collect();

        PoolSnapshot {
            blocks_total: self.pieces.blocks_total(),
            blocks_done: self.pieces.blocks_received(),
            blocks_in_flight: self.block_assignments.len(),
            peers,
        }
    }

    pub fn step(&mut self, input: Input) -> Vec<Output> {
        match input {
            Input::PeersDiscovered(addrs) => self.on_discovered(addrs),
            Input::PeerConnected { addr, peer_id } => self.on_connected(addr, peer_id),
            Input::PeerDisconnected(addr) => self.on_disconnected(addr),
            Input::MessageReceived { addr, message } => self.on_message(addr, message),
            Input::Tick => self.schedule_requests(),
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

    fn on_connected(&mut self, addr: SocketAddr, peer_id: PeerId) -> Vec<Output> {
        // Also handles future inbound connections not preceded by PeersDiscovered.
        let state = self
            .peers
            .entry(addr)
            .or_insert_with(|| PeerState::new(self.metainfo.pieces.len()));
        state.peer_id = Some(peer_id);
        self.interested_or_request(addr)
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
            self.pieces.reset_block(block_ref);
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
            Message::Piece {
                piece_index,
                piece_offset,
                data,
            } => {
                let block_ref = BlockRef {
                    piece_index,
                    piece_offset,
                };
                self.on_message_piece(addr, block_ref, data)
            },
            Message::KeepAlive => vec![],
            Message::Interested => vec![Output::SendToPeer {
                addr,
                message: Message::Unchoke,
            }],
            Message::NotInterested => vec![],
            Message::Request { .. } => vec![],
            Message::Cancel { .. } => vec![],
            Message::Unimplemented => vec![],
        }
    }

    /// Ask the peer to unchoke us (Send Interrested), or send block requests if
    /// already unchocked
    fn interested_or_request(&mut self, addr: SocketAddr) -> Vec<Output> {
        let peer = self.peers.get_mut(&addr).expect("peer must be available");
        if !peer.am_interested {
            peer.am_interested = true;
            // Signal interest unconditionally — peer will unchoke us if they agree.
            return vec![Output::SendToPeer {
                addr,
                message: Message::Interested,
            }];
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
        self.availability
            .entry(piece_index)
            .or_default()
            .insert(addr);
        self.interested_or_request(addr)
    }

    fn on_message_choke(&mut self, addr: SocketAddr) -> Vec<Output> {
        let orphaned: Vec<BlockRef> = self
            .block_assignments
            .iter()
            .filter(|(_, peer)| **peer == addr)
            .map(|(k, _)| *k)
            .collect();
        for block_ref in orphaned {
            self.block_assignments.remove(&block_ref);
            self.pieces.reset_block(block_ref);
        }
        if let Some(state) = self.peers.get_mut(&addr) {
            state.in_flight = 0;
        }
        self.schedule_requests()
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
                PieceEvent::PieceCompleted {
                    piece_index,
                    piece_offset,
                    data,
                } => {
                    let mut outputs = vec![
                        Output::Broadcast(Message::Have(piece_index)),
                        Output::WritePiece {
                            offset: piece_offset,
                            data,
                        },
                    ];
                    if self.pieces.is_complete() {
                        outputs.push(Output::Completed);
                        return outputs;
                    }
                    outputs.extend(self.schedule_requests());
                    outputs
                },
            },
        }
    }

    fn has_scheduling_capacity(&self) -> bool {
        self.peers.values().any(|s| s.can_accept_request())
    }

    fn pick_peer(&self, peer_addrs: &[SocketAddr], rng: &mut impl rand::Rng) -> Option<SocketAddr> {
        peer_addrs
            .iter()
            .filter(|addr| {
                self.peers
                    .get(addr)
                    .map_or(false, |s| s.can_accept_request())
            })
            .choose(rng)
            .copied()
    }

    fn schedule_requests(&mut self) -> Vec<Output> {
        if !self.has_scheduling_capacity() {
            return vec![];
        }

        let mut rng = rand::rng();
        let mut peer_addrs: Vec<SocketAddr> = Vec::new();
        let mut outputs = vec![];

        // Needed pieces sorted by rarest first
        let mut needed: Vec<usize> = self.pieces.needed_pieces().collect();
        needed.sort_by_key(|&piece| {
            self.availability
                .get(&piece)
                .map_or(usize::MAX, |peers| peers.len())
        });

        for piece_index in needed {
            // Collect owned addrs — releases the borrow on self.availability before
            // the inner loop mutates self.peers.
            peer_addrs.clear(); // keep allocated capacity
            match self.availability.get(&piece_index) {
                Some(peers) => peer_addrs.extend(peers.iter().copied()),
                None => continue,
            }

            let missing: Vec<BlockRange> = self.pieces.missing_blocks(piece_index).collect();
            for block_range in missing {
                let block_ref = BlockRef::from(&block_range);
                if let Some(old_peer) = self.block_assignments.get(&block_ref) {
                    if let Some(state) = self.peers.get_mut(&old_peer) {
                        state.in_flight = state.in_flight.saturating_sub(1);
                    }
                }

                let candidate = self.pick_peer(&peer_addrs, &mut rng);
                if let Some(addr) = candidate {
                    self.peers.get_mut(&addr).unwrap().in_flight += 1;
                    self.block_assignments.insert(block_ref, addr);
                    let _ = self.pieces.request_block(block_ref);

                    outputs.push(Output::SendToPeer {
                        addr,
                        message: Message::Request {
                            piece_index: block_range.piece_index,
                            piece_offset: block_range.piece_offset,
                            piece_len: block_range.piece_len,
                        },
                    });
                }
            }
        }

        outputs
    }
}

struct PeerState {
    peer_id: Option<PeerId>,
    // am_choking: bool,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
    bitfield: Bitfield,
    in_flight: usize,
}

impl PeerState {
    /// Allow 16kb * 32 = 512kb max in transit - not aggressif for peers
    const MAX_IN_FLIGHT_PER_PEER: usize = 32;

    fn can_accept_request(&self) -> bool {
        !self.peer_choking && self.in_flight < Self::MAX_IN_FLIGHT_PER_PEER
    }

    fn new(pieces: usize) -> Self {
        Self {
            peer_id: None,
            // am_choking: true,
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
            Message::KeepAlive => {},
            Message::Request { .. } => {},
            Message::Piece { .. } => {},
            Message::Cancel { .. } => {},
            Message::Unimplemented => {},
        }
    }
}
