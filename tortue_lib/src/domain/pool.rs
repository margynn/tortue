use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    net::SocketAddr,
    sync::Arc,
    vec,
};

use rand::seq::IteratorRandom;

use crate::domain::{
    message::{UT_METADATA_EXT_ID, UtMetadataMessage},
    peer::PeerExtensions,
};

use super::{
    bitfield::Bitfield,
    message::{ExtensionHandshake, Message},
    peer::PeerId,
    pieces::{BlockRange, BlockRef, PieceEvent, PieceManager},
    torrent::Metainfo,
};

pub enum Input {
    PeersDiscovered(Vec<SocketAddr>),
    PeerConnected {
        addr: SocketAddr,
        peer_id: PeerId,
        peer_extensions: PeerExtensions,
    },
    PeerDisconnected(SocketAddr),
    MessageReceived {
        addr: SocketAddr,
        message: Message,
    },
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
    pub state: PeerState,
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
                state: s.clone(),
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
            Input::PeerConnected {
                addr,
                peer_id,
                peer_extensions,
            } => self.on_connected(addr, peer_id, peer_extensions),
            Input::PeerDisconnected(addr) => self.on_disconnected(addr),
            Input::MessageReceived { addr, message } => self.on_message(addr, message),
            Input::Tick => self.schedule_requests(),
        }
    }

    fn on_discovered(&mut self, socket_addrs: Vec<SocketAddr>) -> Vec<Output> {
        let mut output = vec![];
        for addr in socket_addrs {
            if let Entry::Vacant(..) = self.peers.entry(addr) {
                output.push(Output::ConnectPeer(addr));
            }
        }
        output
    }

    fn release_peer_blocks(&mut self, addr: SocketAddr) {
        let orphaned: Vec<BlockRef> = self
            .block_assignments
            .iter()
            .filter(|(_, p)| **p == addr)
            .map(|(k, _)| *k)
            .collect();
        for block_ref in orphaned {
            self.block_assignments.remove(&block_ref);
            self.pieces.reset_block(block_ref);
        }
    }

    fn on_connected(
        &mut self,
        addr: SocketAddr,
        peer_id: PeerId,
        extensions: PeerExtensions,
    ) -> Vec<Output> {
        self.release_peer_blocks(addr);

        let pieces = self.metainfo.pieces.len();
        self.peers
            .entry(addr)
            .insert_entry(PeerState::new(addr, peer_id, pieces, extensions));

        self.release_peer_blocks(addr);
        self.interested_or_request(addr)
    }

    fn on_disconnected(&mut self, addr: SocketAddr) -> Vec<Output> {
        self.peers.remove(&addr);
        self.availability.retain(|_, peers| {
            peers.remove(&addr);
            !peers.is_empty()
        });
        self.release_peer_blocks(addr);
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
            Message::Request { .. } => vec![], // TODO: send bytes
            Message::Cancel { .. } => vec![],
            Message::ExtensionHandshake(_) => vec![],
            Message::Extension { ext_id, payload } => {
                self.on_extension_message(addr, ext_id, &payload)
            },
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
        self.release_peer_blocks(addr);
        self.schedule_requests()
    }

    fn on_message_piece(
        &mut self,
        _addr: SocketAddr,
        block_ref: BlockRef,
        data: Vec<u8>,
    ) -> Vec<Output> {
        if self.block_assignments.remove(&block_ref).is_none() {
            return vec![];
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

    fn on_extension_message(&self, addr: SocketAddr, ext_id: u8, payload: &[u8]) -> Vec<Output> {
        let state = self.peers.get(&addr).expect("expect peer");
        let peer_ext_id = state
            .extensions
            .as_ref()
            .and_then(|hs| hs.extensions.get("ut_metadata").copied());
        let Some(peer_ext_id) = peer_ext_id else {
            return vec![];
        };

        match ext_id {
            UT_METADATA_EXT_ID => match UtMetadataMessage::decode(payload) {
                Ok(UtMetadataMessage::Request { piece }) => {
                    let data = self.metainfo.info_bytes_block(piece);
                    let response = UtMetadataMessage::Data {
                        piece,
                        total_size: self.metainfo.info_bytes.len(),
                        data,
                    };
                    vec![Output::SendToPeer {
                        addr,
                        message: Message::Extension {
                            ext_id: peer_ext_id,
                            payload: response.encode(),
                        },
                    }]
                },
                _ => vec![],
            },

            // TODO: add more extension message here
            _ => vec![],
        }
    }

    const MAX_IN_FLIGHT_PER_PEER: usize = 32;

    fn pick_peer<'a>(
        &self,
        peer_addrs: &'a [SocketAddr],
        in_flight: &HashMap<SocketAddr, usize>,
        rng: &mut impl rand::Rng,
    ) -> Option<&'a SocketAddr> {
        peer_addrs
            .iter()
            .filter(|addr| {
                self.peers.get(addr).map_or(false, |s| {
                    !s.peer_choking
                        && in_flight.get(addr).copied().unwrap_or(0) < Self::MAX_IN_FLIGHT_PER_PEER
                })
            })
            .choose(rng)
    }

    fn schedule_requests(&mut self) -> Vec<Output> {
        // Build in_flight counts once — O(blocks) instead of O(blocks × peers)
        let mut in_flight: HashMap<SocketAddr, usize> = HashMap::new();
        for &peer in self.block_assignments.values() {
            *in_flight.entry(peer).or_default() += 1;
        }

        let can_schedule = self.peers.iter().any(|(addr, s)| {
            !s.peer_choking
                && in_flight.get(addr).copied().unwrap_or(0) < Self::MAX_IN_FLIGHT_PER_PEER
        });
        if !can_schedule {
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

                if let Some(&addr) = self.pick_peer(&peer_addrs, &in_flight, &mut rng) {
                    *in_flight.entry(addr).or_default() += 1;
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

#[derive(Clone)]
struct PeerState {
    addr: SocketAddr,
    peer_id: PeerId,
    am_choking: bool,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
    bitfield: Bitfield,
    dht: bool,
    fast: bool,
    extensions: Option<ExtensionHandshake>, // BEP 10
}

impl PeerState {
    fn new(addr: SocketAddr, peer_id: PeerId, pieces: usize, extensions: PeerExtensions) -> Self {
        Self {
            addr,
            peer_id,
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            bitfield: Bitfield::new(pieces),
            dht: extensions.dht,
            fast: extensions.fast,
            extensions: None,
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
            Message::ExtensionHandshake(hs) => self.extensions = Some(hs.clone()),
            Message::Extension { .. } => {},
        }
    }
}
