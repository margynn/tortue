mod block_assignment;
mod peer_registry;

use std::{collections::hash_map::Entry, net::SocketAddr, sync::Arc, vec};

use rand::seq::IteratorRandom;

use block_assignment::BlockAssignments;

use crate::domain::coordinator::peer_registry::PeerRegistry;

use super::{
    bitfield::Bitfield,
    message::{Message, UT_METADATA_EXT_ID, UtMetadataMessage},
    peer::{PeerExtensions, PeerId},
    pieces::{BlockRange, BlockRef, PieceEvent, PieceManager},
    torrent::Metainfo,
};

pub(super) type PieceIndex = usize;

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
    DisconnectPeer(SocketAddr),
    SendToPeer { addr: SocketAddr, message: Message },
    WritePiece { offset: u64, data: Vec<u8> },
    Broadcast(Message),
    Completed,
}

pub struct Coordinator {
    metainfo: Arc<Metainfo>,
    peer_registry: PeerRegistry,
    block_assignments: BlockAssignments,
    pieces: PieceManager,
}

pub struct CoordinatorSnapshot {
    pub blocks_total: usize,
    pub blocks_done: usize,
    pub blocks_in_flight: usize,
    pub peers: Vec<SocketAddr>,
}

impl Coordinator {
    pub fn new(metainfo: Arc<Metainfo>) -> Self {
        let pieces = PieceManager::new(Arc::clone(&metainfo));
        Self {
            metainfo,
            peer_registry: PeerRegistry::new(),
            block_assignments: BlockAssignments::new(),
            pieces,
        }
    }

    pub fn snapshot(&self) -> CoordinatorSnapshot {
        CoordinatorSnapshot {
            blocks_total: self.pieces.blocks_total(),
            blocks_done: self.pieces.blocks_received(),
            blocks_in_flight: self.block_assignments.requests_in_flight(),
            peers: self.peers.keys().map(|addr| *addr).collect(),
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
            Input::Tick => self.on_tick(),
        }
    }

    fn on_tick(&mut self) -> Vec<Output> {
        // Sweeping here rather than in `schedule_requests` keeps the per-message
        // path free of a walk over every request in flight; a 30s timeout does
        // not need finer granularity than a tick.
        for block_ref in self.block_assignments.release_expired() {
            self.reset_if_orphaned(block_ref);
        }
        self.schedule_requests()
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
        for block_ref in self.block_assignments.release_peer(addr) {
            self.reset_if_orphaned(block_ref);
        }
    }

    /// Endgame leaves several peers holding the same block, so a single one
    /// dropping out must not make it `Missing` again.
    fn reset_if_orphaned(&mut self, block_ref: BlockRef) {
        if !self.block_assignments.has_holder(block_ref) {
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

        // Communicate the pieces we have — BEP 6 allows exactly one of
        // HaveAll/HaveNone/Bitfield, never a Bitfield on top of the other two.
        let message = if extensions.fast && self.pieces.is_complete() {
            Message::HaveAll
        } else if extensions.fast && self.pieces.is_empty() {
            Message::HaveNone
        } else {
            Message::Bitfield(self.pieces.bitfield.clone().into())
        };
        let mut out = vec![Output::SendToPeer { addr, message }];
        out.extend(self.interested_or_request(addr));
        out
    }

    fn on_disconnected(&mut self, addr: SocketAddr) -> Vec<Output> {
        self.peers.remove(&addr);
        self.availability.remove_peer(addr);
        self.release_peer_blocks(addr);
        self.schedule_requests()
    }

    fn on_message(&mut self, addr: SocketAddr, message: Message) -> Vec<Output> {
        let Some(state) = self.peers.get_mut(&addr) else {
            return vec![];
        };
        state.apply(&message);

        // BEP 6 Safety
        match message {
            Message::HaveAll
            | Message::HaveNone
            | Message::SuggestPiece(_)
            | Message::RejectRequest { .. }
            | Message::AllowedFast(_)
                if !state.fast =>
            {
                return vec![Output::DisconnectPeer(addr)];
            },
            _ => {},
        };

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
            Message::Interested => vec![Output::SendToPeer {
                addr,
                message: Message::Unchoke,
            }],
            Message::NotInterested => vec![],
            Message::Request {
                piece_index,
                piece_offset,
                piece_len,
            } => self.on_message_request(addr, piece_index, piece_offset, piece_len),
            Message::Cancel { .. } => vec![],
            Message::KeepAlive => vec![],
            Message::Unimplemented => vec![],

            // BEP 10
            Message::ExtensionHandshake(_) => vec![],
            Message::Extension { ext_id, payload } => {
                self.on_extension_message(addr, ext_id, &payload)
            },

            // BEP 6
            Message::HaveAll => {
                for piece_index in 0..self.metainfo.pieces.len() {
                    self.availability.record(piece_index, addr);
                }
                self.interested_or_request(addr)
            },
            Message::HaveNone => vec![],
            Message::SuggestPiece(piece_index) => self.on_message_suggest_piece(addr, piece_index),
            Message::RejectRequest {
                piece_index,
                piece_offset,
                ..
            } => {
                let block_ref = BlockRef {
                    piece_index,
                    piece_offset,
                };
                self.block_assignments.unassign(block_ref, addr);
                self.reset_if_orphaned(block_ref);
                self.interested_or_request(addr)
            },
            Message::AllowedFast(_) => self.interested_or_request(addr),
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
        if peer.peer_choking && peer.allowed_fast.is_empty() {
            return vec![]; // Already interested, waiting for unchoke.
        }
        self.schedule_requests()
    }

    fn on_message_bitfield(&mut self, addr: SocketAddr, bits: Vec<u8>) -> Vec<Output> {
        // Record the pieces available at peer
        if let Ok(bf) = Bitfield::try_from(bits.as_ref()) {
            for piece in &bf {
                self.availability.record(piece, addr);
            }
        }
        self.interested_or_request(addr)
    }

    fn on_message_have(&mut self, addr: SocketAddr, piece_index: usize) -> Vec<Output> {
        self.availability.record(piece_index, addr);
        self.interested_or_request(addr)
    }

    fn on_message_choke(&mut self, addr: SocketAddr) -> Vec<Output> {
        self.release_peer_blocks(addr);
        self.interested_or_request(addr)
    }

    fn on_message_request(
        &mut self,
        addr: SocketAddr,
        piece_index: usize,
        piece_offset: usize,
        piece_len: usize,
    ) -> Vec<Output> {
        let Some(data) = self.pieces.read_block(piece_index, piece_offset, piece_len) else {
            return vec![];
        };
        vec![Output::SendToPeer {
            addr,
            message: Message::Piece {
                piece_index,
                piece_offset,
                data,
            },
        }]
    }

    fn on_message_piece(
        &mut self,
        addr: SocketAddr,
        block_ref: BlockRef,
        data: Vec<u8>,
    ) -> Vec<Output> {
        // Only a peer we actually requested this block from may fulfil it —
        // otherwise any connected peer could complete blocks assigned to others.
        if !self.block_assignments.is_holder(block_ref, addr) {
            return vec![];
        }
        self.block_assignments.unassign(block_ref, addr);

        match self.pieces.receive_block(block_ref, data) {
            Err(_) => {
                // Malformed block: make it requestable again instead of leaving
                // it Requested until the timeout expires.
                self.reset_if_orphaned(block_ref);
                vec![]
            },
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

    fn on_message_suggest_piece(&mut self, addr: SocketAddr, piece_index: usize) -> Vec<Output> {
        if !self.pieces.needed_pieces().any(|p| p == piece_index) {
            return vec![]; // already have it
        }
        if !self
            .peers
            .get(&addr)
            .is_some_and(|s| s.can_serve(piece_index))
        {
            return vec![]; // peer chokes us and hasn't allow-fasted this piece
        }

        let blocks: Vec<BlockRange> = self.pieces.unreceived_blocks(piece_index).collect();
        let mut outputs = vec![];
        for block_range in blocks {
            if !self.block_assignments.has_capacity(addr) {
                break;
            }
            if self
                .block_assignments
                .is_holder(BlockRef::from(&block_range), addr)
            {
                continue;
            }
            outputs.push(self.send_request(addr, block_range));
        }
        outputs
    }

    fn schedule_requests(&mut self) -> Vec<Output> {
        let mut budget = self.request_budget();
        if budget == 0 {
            return vec![];
        }

        let mut rng = rand::rng();
        let mut peer_addrs: Vec<SocketAddr> = Vec::new();
        let mut outputs = vec![];

        // Needed pieces sorted by rarest first. Cached key: `rarity` hits a
        // HashMap, and `sort_by_key` would re-evaluate it on every comparison.
        let mut needed: Vec<usize> = self.pieces.needed_pieces().collect();
        needed.sort_by_cached_key(|&piece| self.availability.rarity(piece));

        // One sweep per holder count, so no block gets a second peer while
        // another still has none. Sweeping again while budget remains is what
        // spends the tail of the download on duplicates instead of idle slots,
        // and it is self-limiting: a sweep only reaches depth 1 once every
        // unreceived block has a holder, which caps their number at the requests
        // in flight. A block can be held by at most every peer.
        for depth in 0..self.peers.len() {
            let mut assigned = 0;

            for &piece_index in &needed {
                if budget == 0 {
                    break;
                }

                // Collect owned addrs — releases the borrow on self.availability
                // before the inner loop mutates self.peers.
                peer_addrs.clear(); // keep allocated capacity
                peer_addrs.extend(self.availability.peers_for(piece_index).copied());
                if peer_addrs.is_empty() {
                    continue;
                }

                let blocks: Vec<BlockRange> = self.pieces.unreceived_blocks(piece_index).collect();

                for block_range in blocks {
                    if budget == 0 {
                        break;
                    }
                    let block_ref = BlockRef::from(&block_range);
                    if self.block_assignments.holder_count(block_ref) != depth {
                        continue;
                    }
                    if let Some(&addr) = self.pick_peer(&peer_addrs, block_ref, &mut rng) {
                        budget -= 1;
                        assigned += 1;
                        outputs.push(self.send_request(addr, block_range));
                    }
                }
            }

            if budget == 0 || assigned == 0 {
                break;
            }
        }

        outputs
    }

    fn send_request(&mut self, addr: SocketAddr, block_range: BlockRange) -> Output {
        let block_ref = BlockRef::from(&block_range);
        self.block_assignments.assign(block_ref, addr);
        let _ = self.pieces.request_block(block_ref);
        Output::SendToPeer {
            addr,
            message: Message::Request {
                piece_index: block_range.piece_index,
                piece_offset: block_range.piece_offset,
                piece_len: block_range.piece_len,
            },
        }
    }

    fn pick_peer<'a>(
        &self,
        peer_addrs: &'a [SocketAddr],
        block_ref: BlockRef,
        rng: &mut impl rand::Rng,
    ) -> Option<&'a SocketAddr> {
        peer_addrs
            .iter()
            .filter(|addr| {
                self.peers
                    .get(addr)
                    .is_some_and(|s| s.can_serve(block_ref.piece_index))
                    && self.block_assignments.has_capacity(**addr)
                    && !self.block_assignments.is_holder(block_ref, **addr)
            })
            .choose(rng)
    }

    fn request_budget(&self) -> usize {
        self.peers
            .values()
            .filter(|s| !s.peer_choking)
            .map(|s| self.block_assignments.free_slots_for(s.addr))
            .sum()
    }
}
