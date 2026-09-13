use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

use super::{PieceIndex, block_assignment::BlockAssignments};
use crate::domain::{
    bitfield::Bitfield,
    message::{ExtensionHandshake, Message},
    peer::PeerExtensions,
};

pub(super) struct PeerRegistry {
    peers: HashMap<SocketAddr, PeerState>,
    availability: PieceAvailability,
}

impl PeerRegistry {
    pub(super) fn new(total_pieces: usize) -> Self {
        Self {
            peers: HashMap::new(),
            availability: PieceAvailability::new(total_pieces),
        }
    }

    pub(super) fn connected(&mut self, addr: SocketAddr, extensions: PeerExtensions) {
        self.peers.insert(addr, PeerState::new(extensions));
    }

    pub(super) fn disconnected(&mut self, addr: SocketAddr) {
        self.peers.remove(&addr);
        self.availability.remove_peer(addr);
    }

    pub(super) fn apply(&mut self, addr: SocketAddr, msg: &Message) {
        let Some(peer) = self.peers.get_mut(&addr) else {
            return;
        };
        peer.apply(msg);

        match msg {
            Message::Bitfield(bits) => {
                // Drop previous one
                self.availability.remove_peer(addr);
                if let Ok(bf) = Bitfield::try_from(bits.as_ref()) {
                    for piece in &bf {
                        self.availability.record(piece, addr);
                    }
                }
            },
            Message::Have(piece) => self.availability.record(*piece, addr),
            Message::HaveAll => self.availability.record_all(addr),
            Message::HaveNone => self.availability.remove_peer(addr),
            _ => {},
        }
    }

    pub(super) fn peers_with(&self, piece: usize) -> impl Iterator<Item = SocketAddr> + '_ {
        self.availability.peers_for(piece).copied()
    }

    pub(super) fn rarity(&self, piece: usize) -> usize {
        self.availability.rarity(piece)
    }

    pub(super) fn can_serve(&self, addr: SocketAddr, piece: usize) -> bool {
        self.peers.get(&addr).is_some_and(|s| s.can_serve(piece))
    }

    pub(super) fn unchoked(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        self.peers
            .iter()
            .filter(|(_, s)| !s.peer_choking)
            .map(|(&addr, _)| addr)
    }

    pub(super) fn budget(&self, assignments: &BlockAssignments) -> usize {
        self.unchoked()
            .map(|addr| assignments.free_slots_for(addr))
            .sum()
    }
}

struct PieceAvailability {
    by_piece: HashMap<PieceIndex, HashSet<SocketAddr>>,
    total_pieces: usize,
}

impl PieceAvailability {
    fn new(total_pieces: usize) -> Self {
        Self {
            by_piece: HashMap::new(),
            total_pieces,
        }
    }

    fn record(&mut self, piece_index: PieceIndex, addr: SocketAddr) {
        self.by_piece.entry(piece_index).or_default().insert(addr);
    }

    fn record_all(&mut self, addr: SocketAddr) {
        for piece in 0..self.total_pieces {
            self.record(piece, addr);
        }
    }

    fn remove_peer(&mut self, addr: SocketAddr) {
        self.by_piece.retain(|_, peers| {
            peers.remove(&addr);
            !peers.is_empty()
        });
    }

    /// Peers known to have `piece_index` (empty iterator if none known).
    fn peers_for(&self, piece_index: PieceIndex) -> impl Iterator<Item = &SocketAddr> {
        self.by_piece.get(&piece_index).into_iter().flatten()
    }

    /// Rarity for sorting: fewer known peers = rarer. Unknown pieces sort last.
    fn rarity(&self, piece_index: PieceIndex) -> usize {
        self.by_piece
            .get(&piece_index)
            .map_or(usize::MAX, |peers| peers.len())
    }
}

#[derive(Clone)]
struct PeerState {
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
    allowed_fast: HashSet<usize>,
    fast: bool,
    extensions: Option<ExtensionHandshake>, // BEP 10
}

impl PeerState {
    fn new(extensions: PeerExtensions) -> Self {
        Self {
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            allowed_fast: HashSet::new(),
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
            Message::ExtensionHandshake(hs) => self.extensions = Some(hs.clone()),
            Message::AllowedFast(piece) => {
                self.allowed_fast.insert(*piece);
            },
            _ => {},
        }
    }

    fn can_serve(&self, piece_index: usize) -> bool {
        !self.peer_choking || self.allowed_fast.contains(&piece_index)
    }
}
