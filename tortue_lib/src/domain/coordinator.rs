mod block_assignment;
mod peer_registry;
mod pieces;

use std::{net::SocketAddr, sync::Arc, vec};

use rand::seq::IteratorRandom;

use block_assignment::BlockAssignments;
use peer_registry::{PeerRegistry, RejectReason};
use pieces::{BlockRange, BlockRef, CompletedPiece, PieceManager};

use super::{
    message::{Message, UT_METADATA_EXT_ID, UtMetadataMessage},
    peer::PeerExtensions,
    torrent::Metainfo,
};

pub(super) type PieceIndex = usize;

pub enum Input {
    PeersDiscovered(Vec<SocketAddr>),
    PeerConnected {
        addr: SocketAddr,
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
        let total_pieces = metainfo.pieces.len();
        Self {
            metainfo: Arc::clone(&metainfo),
            peer_registry: PeerRegistry::new(total_pieces),
            block_assignments: BlockAssignments::new(),
            pieces: PieceManager::new(Arc::clone(&metainfo)),
        }
    }

    pub fn snapshot(&self) -> CoordinatorSnapshot {
        CoordinatorSnapshot {
            blocks_total: self.pieces.blocks_total(),
            blocks_done: self.pieces.blocks_received(),
            blocks_in_flight: self.block_assignments.requests_in_flight(),
            peers: self.peer_registry.addrs().collect(),
        }
    }

    pub fn step(&mut self, input: Input) -> Vec<Output> {
        match input {
            Input::PeersDiscovered(addrs) => self.on_discovered(addrs),
            Input::PeerConnected {
                addr,
                peer_extensions,
            } => self.on_connected(addr, peer_extensions),
            Input::PeerDisconnected(addr) => self.on_disconnected(addr),
            Input::MessageReceived { addr, message } => self.on_message(addr, message),
            Input::Tick => self.on_tick(),
        }
    }

    fn on_tick(&mut self) -> Vec<Output> {
        // Sweeping here rather than in `plan` keeps the per-message path free
        // of a walk over every request in flight; a few seconds of extra
        // latency on a timeout does not need finer granularity than a tick.
        self.block_assignments.release_expired();
        self.plan()
    }

    fn on_discovered(&mut self, socket_addrs: Vec<SocketAddr>) -> Vec<Output> {
        let mut output = vec![];
        for addr in socket_addrs {
            if !self.peer_registry.contains(addr) {
                output.push(Output::ConnectPeer(addr));
            }
        }
        output
    }

    fn on_connected(&mut self, addr: SocketAddr, extensions: PeerExtensions) -> Vec<Output> {
        self.block_assignments.release_peer(addr);
        self.peer_registry.connected(addr, extensions);

        // Communicate the pieces we have — BEP 6 allows exactly one of
        // HaveAll/HaveNone/Bitfield, never a Bitfield on top of the other two.
        let message = if extensions.fast && self.pieces.is_complete() {
            Message::HaveAll
        } else if extensions.fast && self.pieces.has_no_piece() {
            Message::HaveNone
        } else {
            Message::Bitfield(self.pieces.bitfield().clone().into())
        };
        let mut out = vec![Output::SendToPeer { addr, message }];
        out.extend(self.interested_or_request(addr));
        out
    }

    fn on_disconnected(&mut self, addr: SocketAddr) -> Vec<Output> {
        self.peer_registry.disconnected(addr);
        self.block_assignments.release_peer(addr);
        self.plan()
    }

    fn on_message(&mut self, addr: SocketAddr, message: Message) -> Vec<Output> {
        match self.peer_registry.apply(addr, &message) {
            Err(RejectReason::UnknownPeer) => return vec![],
            Err(RejectReason::ProtocolViolation) => return vec![Output::DisconnectPeer(addr)],
            Ok(()) => {},
        }

        match message {
            Message::Bitfield(_) | Message::Have(_) | Message::HaveAll => {
                // Availability already updated by `peer_registry.apply` above.
                self.interested_or_request(addr)
            },
            Message::HaveNone => vec![],
            Message::Unchoke => self.plan(),
            Message::Choke => {
                self.block_assignments.release_peer(addr);
                self.interested_or_request(addr)
            },
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
            } => self.on_message_request(piece_index, piece_offset, piece_len, addr),
            Message::Cancel { .. } => vec![],
            Message::KeepAlive => vec![],
            Message::Unimplemented => vec![],

            // BEP 10
            Message::ExtensionHandshake(_) => vec![],
            Message::Extension { ext_id, payload } => {
                self.on_extension_message(addr, ext_id, &payload)
            },

            // BEP 6 — the hint is already recorded on the peer by
            // `peer_registry.apply`; `plan()` is what acts on it.
            Message::SuggestPiece(_) => self.interested_or_request(addr),
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
                self.interested_or_request(addr)
            },
            Message::AllowedFast(_) => self.interested_or_request(addr),
        }
    }

    /// Ask the peer to unchoke us (send Interested), or request blocks if
    /// already unchoked.
    fn interested_or_request(&mut self, addr: SocketAddr) -> Vec<Output> {
        if let Some(message) = self.peer_registry.declare_interest(addr) {
            return vec![Output::SendToPeer { addr, message }];
        }
        if !self.peer_registry.is_requestable(addr) {
            return vec![]; // Already interested, waiting for unchoke.
        }
        self.plan()
    }

    fn on_message_request(
        &mut self,
        piece_index: usize,
        piece_offset: usize,
        piece_len: usize,
        addr: SocketAddr,
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

        let Ok(completed) = self.pieces.receive_block(block_ref, data) else {
            return vec![]; // Malformed block: already unassigned, gets replanned.
        };
        let Some(CompletedPiece {
            piece_index,
            piece_offset,
            data,
        }) = completed
        else {
            return self.plan();
        };

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
        outputs.extend(self.plan());
        outputs
    }

    fn on_extension_message(&self, addr: SocketAddr, ext_id: u8, payload: &[u8]) -> Vec<Output> {
        let Some(peer_ext_id) = self.peer_registry.peer_extension_id(addr, "ut_metadata") else {
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

    /// The scheduling policy: suggested pieces first, then rarest, and within
    /// a piece always the block with fewest holders.
    ///
    /// `holders` is built once (O(N) over in-flight requests) instead of
    /// rescanned per block per depth. One sweep per depth, so no block gets a
    /// second holder while another still has none; a sweep only reaches
    /// depth d once every block has at least d holders, which caps the
    /// number of sweeps at the number of peers. A sweep that assigns nothing
    /// means no deeper one can either, so it stops there.
    fn plan(&mut self) -> Vec<Output> {
        let mut budget = self.budget();
        if budget == 0 {
            return vec![];
        }

        let mut holders = self.block_assignments.holder_counts();
        let mut needed: Vec<usize> = self.pieces.needed_pieces().collect();
        needed.sort_by_cached_key(|&piece| {
            (
                !self.peer_registry.is_suggested(piece),
                self.peer_registry.rarity(piece),
            )
        });
        let blocks: Vec<BlockRange> = needed
            .iter()
            .flat_map(|&piece| self.pieces.unreceived_blocks(piece))
            .collect();

        let mut rng = rand::rng();
        let mut outputs = vec![];
        for depth in 0.. {
            let mut assigned = 0;

            for &block in &blocks {
                if budget == 0 {
                    break;
                }
                if holders.get(&block.block).copied().unwrap_or(0) != depth {
                    continue;
                }
                let Some(addr) = self.pick_peer(block.block, &mut rng) else {
                    continue; // Nobody can serve this block right now.
                };

                budget -= 1;
                assigned += 1;
                *holders.entry(block.block).or_insert(0) += 1;
                outputs.push(self.send_request(addr, block));
            }

            if budget == 0 || assigned == 0 {
                break;
            }
        }

        outputs
    }

    /// Total request slots free across peers who unchoked us.
    fn budget(&self) -> usize {
        self.peer_registry
            .unchoked()
            .map(|addr| self.block_assignments.free_slots_for(addr))
            .sum()
    }

    fn pick_peer(&self, block_ref: BlockRef, rng: &mut impl rand::Rng) -> Option<SocketAddr> {
        self.peer_registry
            .peers_with(block_ref.piece_index)
            .filter(|&addr| {
                self.peer_registry.can_serve(addr, block_ref.piece_index)
                    && self.block_assignments.free_slots_for(addr) > 0
                    && !self.block_assignments.is_holder(block_ref, addr)
            })
            .choose(rng)
    }

    fn send_request(&mut self, addr: SocketAddr, block: BlockRange) -> Output {
        self.block_assignments.assign(block.block, addr);
        Output::SendToPeer {
            addr,
            message: Message::Request {
                piece_index: block.block.piece_index,
                piece_offset: block.block.piece_offset,
                piece_len: block.len,
            },
        }
    }
}
