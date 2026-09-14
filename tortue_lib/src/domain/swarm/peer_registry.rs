use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

use super::PieceIndex;
use crate::domain::{
    bitfield::Bitfield,
    message::{ExtensionHandshake, Message},
    peer::PeerExtensions,
};

pub(super) struct PeerRegistry {
    peers: HashMap<SocketAddr, PeerState>,
    availability: PieceAvailability,
    pub seeders: HashSet<SocketAddr>,
    pub leechers: HashSet<SocketAddr>,
}

#[derive(Debug)]
pub(super) enum RejectReason {
    UnknownPeer,
    /// A BEP6 message from a peer that never negotiated the Fast Extension.
    ProtocolViolation,
}

impl PeerRegistry {
    pub(super) fn new(total_pieces: usize) -> Self {
        Self {
            peers: HashMap::new(),
            availability: PieceAvailability::new(total_pieces),
            seeders: HashSet::new(),
            leechers: HashSet::new(),
        }
    }

    pub(super) fn connected(&mut self, addr: SocketAddr, extensions: PeerExtensions) {
        self.peers.insert(addr, PeerState::new(extensions));
    }

    pub(super) fn disconnected(&mut self, addr: SocketAddr) {
        self.seeders.remove(&addr);
        self.leechers.remove(&addr);
        self.peers.remove(&addr);
        self.availability.remove_peer(addr);
    }

    pub(super) fn contains(&self, addr: SocketAddr) -> bool {
        self.peers.contains_key(&addr)
    }

    /// `Some(Message::Interested)` the first time we become interested in
    /// `addr` — the caller relays it. `None` on every later call.
    pub(super) fn declare_interest(&mut self, addr: SocketAddr) -> Option<Message> {
        let peer = self.peers.get_mut(&addr)?;
        if peer.am_interested {
            return None;
        }
        peer.am_interested = true;
        Some(Message::Interested)
    }

    pub(super) fn peer_extension_id(&self, addr: SocketAddr, name: &str) -> Option<u8> {
        self.peers
            .get(&addr)?
            .extensions
            .as_ref()?
            .extensions
            .get(name)
            .copied()
    }

    /// Applies `msg` to `addr`'s state, rejecting it before any mutation
    /// happens if the peer is unknown or the message violates BEP6.
    pub(super) fn apply(&mut self, addr: SocketAddr, msg: &Message) -> Result<(), RejectReason> {
        let Some(peer) = self.peers.get_mut(&addr) else {
            return Err(RejectReason::UnknownPeer);
        };
        if msg.needs_fast() && !peer.fast {
            return Err(RejectReason::ProtocolViolation);
        }
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

        if self.availability.is_seeder(addr) {
            self.seeders.insert(addr);
            self.leechers.remove(&addr);
        } else {
            self.leechers.insert(addr);
            self.seeders.remove(&addr);
        }

        Ok(())
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

    /// `true` if any known peer has ever hinted (BEP6 `SuggestPiece`) that it
    /// would serve `piece` well — advisory, biases scheduling but never gates it.
    pub(super) fn is_suggested(&self, piece: usize) -> bool {
        self.peers.values().any(|p| p.suggested.contains(&piece))
    }

    pub(super) fn unchoked(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        self.peers
            .iter()
            .filter(|(_, s)| !s.peer_choking)
            .map(|(&addr, _)| addr)
    }

    pub(super) fn peer_stats(&self) -> impl Iterator<Item = (SocketAddr, u64, u64)> + '_ {
        self.peers
            .iter()
            .map(|(&addr, s)| (addr, s.bytes_uploaded, s.bytes_downloaded))
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

    fn has(&self, piece: PieceIndex, addr: SocketAddr) -> bool {
        self.by_piece
            .get(&piece)
            .is_some_and(|peers| peers.contains(&addr))
    }

    fn is_seeder(&self, addr: SocketAddr) -> bool {
        (0..self.total_pieces).all(|piece| self.has(piece, addr))
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
    suggested: HashSet<usize>,
    fast: bool,
    extensions: Option<ExtensionHandshake>, // BEP 10
    bytes_uploaded: u64,
    bytes_downloaded: u64,
}

impl PeerState {
    fn new(extensions: PeerExtensions) -> Self {
        Self {
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            allowed_fast: HashSet::new(),
            suggested: HashSet::new(),
            fast: extensions.fast,
            extensions: None,
            bytes_downloaded: 0,
            bytes_uploaded: 0,
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
            Message::SuggestPiece(piece) => {
                self.suggested.insert(*piece);
            },
            Message::Piece { data, .. } => {
                self.bytes_downloaded += data.len() as u64;
            },
            Message::Request { piece_len, .. } => {
                self.bytes_uploaded += *piece_len as u64;
            },
            _ => {},
        }
    }

    fn can_serve(&self, piece_index: usize) -> bool {
        !self.peer_choking || self.allowed_fast.contains(&piece_index)
    }
}
