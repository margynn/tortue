use super::super::bitfield::Bitfield;
use super::{Message, PeerId};

#[derive(Debug)]
pub struct ConnectedPeer {
    pub am_choking: bool,
    pub am_interested: bool,
    pub peer_choking: bool,
    pub peer_interested: bool,
    pub bitfield: Bitfield,
}

impl ConnectedPeer {
    fn new(pieces: usize) -> Self {
        Self {
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            bitfield: Bitfield::new(pieces),
        }
    }

    pub fn reset(&mut self, pieces: usize) {
        *self = Self::new(pieces);
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
        }
    }
}

#[derive(Debug)]
pub enum State {
    Disconnected,
    Connected(ConnectedPeer),
}

#[derive(Debug)]
pub enum Input {
    Send(Message),
    Connected { peer_id: PeerId, num_pieces: usize },
    MessageReceived(Message),
    Disconnected,
}

#[derive(Debug, Clone)]
pub enum Output {
    SendToPeer(Message),
    EmitConnected(PeerId),
    EmitDisconnected,
    EmitMessage(Message),
}

pub struct PeerSession {
    state: State,
}

impl PeerSession {
    pub fn new() -> Self {
        Self { state: State::Disconnected }
    }

    pub fn step(&mut self, input: Input) -> Vec<Output> {
        match input {
            Input::Connected { peer_id, num_pieces } => self.on_connected(peer_id, num_pieces),
            Input::MessageReceived(msg) => self.on_message(msg),
            Input::Send(msg) => self.on_send(msg),
            Input::Disconnected => self.on_disconnected(),
        }
    }

    fn on_connected(&mut self, peer_id: PeerId, num_pieces: usize) -> Vec<Output> {
        if !matches!(self.state, State::Disconnected) {
            return vec![];
        }
        self.state = State::Connected(ConnectedPeer::new(num_pieces));
        vec![Output::EmitConnected(peer_id)]
    }

    fn on_message(&mut self, msg: Message) -> Vec<Output> {
        let State::Connected(peer) = &mut self.state else {
            return vec![];
        };
        peer.apply(&msg);
        vec![Output::EmitMessage(msg)]
    }

    fn on_send(&mut self, msg: Message) -> Vec<Output> {
        if !matches!(self.state, State::Connected(_)) {
            return vec![];
        }
        vec![Output::SendToPeer(msg)]
    }

    fn on_disconnected(&mut self) -> Vec<Output> {
        self.state = State::Disconnected;
        vec![Output::EmitDisconnected]
    }
}
