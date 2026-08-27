use std::time::Duration;

use super::{Message, PeerAddr, PeerId};

#[derive(Debug)]
pub enum PeerCommand {
    Shutdown,
    Send(Message),
}

#[derive(Debug)]
pub enum PeerEvent {
    Connected(PeerAddr),
    Disconnected(PeerAddr),
    Message(PeerAddr, Message),
}

#[derive(Debug)]
pub(crate) enum Input {
    CommandReceived(PeerCommand),
    Connected { peer_id: PeerId },
    ConnectionFailed,
    MessageReceived(Message),
    Disconnected,
}

#[derive(Debug)]
pub(crate) enum Output {
    Send(Message),
    EmitEvent(PeerEvent),
    ScheduleRetry(Duration),
    Stop,
}

enum State {
    Disconnected { backoff: Duration },
    Connected,
}

pub(crate) struct PeerSession {
    peer_addr: PeerAddr,
    state: State,
}

impl PeerSession {
    const RECONNECT_DELAY: Duration = Duration::from_secs(2);
    const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);

    pub(crate) fn new(peer_addr: PeerAddr) -> (Self, Vec<Output>) {
        let session = Self {
            peer_addr,
            state: State::Disconnected { backoff: Self::RECONNECT_DELAY },
        };
        // Connect immediately on start
        (session, vec![Output::ScheduleRetry(Duration::ZERO)])
    }

    pub(crate) fn step(&mut self, input: Input) -> Vec<Output> {
        let state = std::mem::replace(
            &mut self.state,
            State::Disconnected { backoff: Duration::ZERO },
        );

        match (state, input) {
            (State::Disconnected { .. }, Input::Connected { .. }) => {
                self.state = State::Connected;
                vec![Output::EmitEvent(PeerEvent::Connected(self.peer_addr))]
            },

            (State::Disconnected { backoff }, Input::ConnectionFailed) => {
                let next_backoff = (backoff * 2).min(Self::MAX_RECONNECT_DELAY);
                self.state = State::Disconnected { backoff: next_backoff };
                vec![Output::ScheduleRetry(backoff)]
            },

            (State::Connected, Input::MessageReceived(msg)) => {
                self.state = State::Connected;
                vec![Output::EmitEvent(PeerEvent::Message(self.peer_addr, msg))]
            },

            (
                State::Connected,
                Input::CommandReceived(PeerCommand::Send(msg)),
            ) => {
                self.state = State::Connected;
                vec![Output::Send(msg)]
            },

            (_, Input::CommandReceived(PeerCommand::Shutdown)) => {
                vec![
                    Output::EmitEvent(PeerEvent::Disconnected(self.peer_addr)),
                    Output::Stop,
                ]
            },

            (_, Input::Disconnected) => {
                self.state =
                    State::Disconnected { backoff: Self::RECONNECT_DELAY };
                vec![
                    Output::EmitEvent(PeerEvent::Disconnected(self.peer_addr)),
                    Output::ScheduleRetry(Self::RECONNECT_DELAY),
                ]
            },

            // Invalid transitions — restore state, ignore input
            (state, _) => {
                self.state = state;
                vec![]
            },
        }
    }
}
