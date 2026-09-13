use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

use super::PieceIndex;

pub(super) struct PieceAvailability {
    by_piece: HashMap<PieceIndex, HashSet<SocketAddr>>,
}

impl PieceAvailability {
    pub(super) fn new() -> Self {
        Self {
            by_piece: HashMap::new(),
        }
    }

    pub(super) fn record(&mut self, piece_index: PieceIndex, addr: SocketAddr) {
        self.by_piece.entry(piece_index).or_default().insert(addr);
    }

    pub(super) fn remove_peer(&mut self, addr: SocketAddr) {
        self.by_piece.retain(|_, peers| {
            peers.remove(&addr);
            !peers.is_empty()
        });
    }

    /// Peers known to have `piece_index` (empty iterator if none known).
    pub(super) fn peers_for(&self, piece_index: PieceIndex) -> impl Iterator<Item = &SocketAddr> {
        self.by_piece.get(&piece_index).into_iter().flatten()
    }

    /// Rarity for sorting: fewer known peers = rarer. Unknown pieces sort last.
    pub(super) fn rarity(&self, piece_index: PieceIndex) -> usize {
        self.by_piece
            .get(&piece_index)
            .map_or(usize::MAX, |peers| peers.len())
    }
}
