use std::{collections::HashSet, net::SocketAddr};

use crate::domain::{
    bitfield::Bitfield,
    message::{ExtensionHandshake, Message},
    peer::{PeerExtensions, PeerId},
};

#[derive(Clone)]
pub(super) struct PeerState {
    pub(super) addr: SocketAddr,
    peer_id: PeerId,
    am_choking: bool,
    pub(super) am_interested: bool,
    pub(super) peer_choking: bool,
    peer_interested: bool,
    bitfield: Bitfield,
    pub(super) allowed_fast: HashSet<usize>,
    dht: bool,
    pub(super) fast: bool,
    pub(super) extensions: Option<ExtensionHandshake>, // BEP 10
}

impl PeerState {
    pub(super) fn new(
        addr: SocketAddr,
        peer_id: PeerId,
        pieces: usize,
        extensions: PeerExtensions,
    ) -> Self {
        Self {
            addr,
            peer_id,
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            bitfield: Bitfield::new(pieces),
            allowed_fast: HashSet::new(),
            dht: extensions.dht,
            fast: extensions.fast,
            extensions: None,
        }
    }

    pub(super) fn apply(&mut self, msg: &Message) {
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
            Message::ExtensionHandshake(hs) => self.extensions = Some(hs.clone()),
            Message::Extension { .. } => {},
            Message::SuggestPiece(_) => {},
            Message::HaveAll => {
                self.bitfield.set_all();
            },
            Message::HaveNone => {
                self.bitfield.unset_all();
            },
            Message::RejectRequest { .. } => {},
            Message::AllowedFast(piece) => {
                self.allowed_fast.insert(*piece);
            },
            Message::Unimplemented => {},
        }
    }

    pub(super) fn can_serve(&self, piece_index: usize) -> bool {
        !self.peer_choking || self.allowed_fast.contains(&piece_index)
    }
}
