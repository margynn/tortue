use crate::peer::bitfield;
use crate::peer::client::Message;

#[derive(Debug)]
#[allow(dead_code)]
pub struct PeerState {
    pub am_choking: bool,
    pub am_interested: bool,
    pub peer_choking: bool,
    pub peer_interested: bool,
    pub bitfield: bitfield::Bitfield,
}

impl PeerState {
    #[allow(dead_code)]
    pub fn new(pieces: usize) -> Self {
        Self {
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            bitfield: bitfield::Bitfield::new(pieces),
        }
    }

    #[allow(dead_code)]
    pub fn reset(&mut self, pieces: usize) {
        *self = Self::new(pieces);
    }

    pub fn apply(&mut self, msg: &Message) {
        match msg {
            Message::Choke => self.peer_choking = true,
            Message::Unchoke => self.peer_choking = false,
            Message::Interested => self.peer_interested = true,
            Message::NotInterested => self.peer_interested = false,
            Message::Bitfield(bits) => {
                if let Ok(bitfield) =
                    bitfield::Bitfield::try_from(bits.as_ref())
                {
                    self.bitfield = bitfield;
                }
            },
            Message::Have(piece) => {
                let _ = self.bitfield.set_bit(*piece as usize);
            },
            Message::KeepAlive | Message::Piece { .. } => {},
            _ => {},
        }
    }
}
