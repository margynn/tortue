use std::net::SocketAddr;
use std::time::Duration;

use super::super::bitfield::Bitfield;
use super::{Message, PeerId};

// #[derive(Debug)]
// pub enum PeerCommand {
//     Shutdown,
//     Send(Message),
// }

// #[derive(Debug)]
// pub enum PeerEvent {
//     Connected(SocketAddr),
//     Disconnected(SocketAddr),
//     Message(SocketAddr, Message),
// }

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
    Disconnected { backoff: Duration },
    Connected(ConnectedPeer),
}

#[derive(Debug)]
pub enum Input {
    Shutdown,
    Send(Message),
    Connected { peer_id: PeerId, num_pieces: usize },
    ConnectionFailed,
    MessageReceived(Message),
    Disconnected,
}

#[derive(Debug, Clone)]
pub enum Output {
    SendToPeer(Message),
    EmitConnected,
    EmitDisconnected,
    EmitMessage(Message),
    ScheduleRetry(Duration),
    Stop,
}

pub struct PeerSession {
    address: SocketAddr,
    state: State,
}

impl PeerSession {
    const RECONNECT_DELAY: Duration = Duration::from_secs(2);
    const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);

    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            state: State::Disconnected { backoff: Self::RECONNECT_DELAY },
        }
    }

    pub fn state(&self) -> &State {
        &self.state
    }

    pub fn step(&mut self, input: Input) -> Vec<Output> {
        match input {
            Input::Connected { num_pieces, .. } => {
                self.on_connected(num_pieces)
            },
            Input::ConnectionFailed => self.on_connection_failed(),
            Input::MessageReceived(msg) => self.on_message(msg),
            Input::Send(msg) => self.on_send(msg),
            Input::Shutdown => self.on_shutdown(),
            Input::Disconnected => self.on_disconnected(),
        }
    }

    fn on_connected(&mut self, num_pieces: usize) -> Vec<Output> {
        if !matches!(self.state, State::Disconnected { .. }) {
            return vec![];
        }
        self.state = State::Connected(ConnectedPeer::new(num_pieces));
        vec![Output::EmitConnected]
    }

    fn on_connection_failed(&mut self) -> Vec<Output> {
        let State::Disconnected { backoff } = self.state else {
            return vec![];
        };
        let next_backoff = (backoff * 2).min(Self::MAX_RECONNECT_DELAY);
        self.state = State::Disconnected { backoff: next_backoff };
        vec![Output::ScheduleRetry(backoff)]
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

    fn on_shutdown(&mut self) -> Vec<Output> {
        vec![Output::EmitDisconnected, Output::Stop]
    }

    fn on_disconnected(&mut self) -> Vec<Output> {
        self.state = State::Disconnected { backoff: Self::RECONNECT_DELAY };
        vec![
            Output::EmitDisconnected,
            Output::ScheduleRetry(Self::RECONNECT_DELAY),
        ]
    }
}
