use std::net::SocketAddr;

use tokio::sync::mpsc;

use crate::domain::{message::Message, peer::PeerEvent};

pub trait PeerConnector: Send + 'static {
    type Inbound: Send + 'static;

    fn connect(
        &mut self,
        addr: SocketAddr,
        cmd_rx: mpsc::Receiver<Message>,
        events_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    );

    fn accept(
        &mut self,
        peer: Self::Inbound,
        cmd_rx: mpsc::Receiver<Message>,
        events_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    );

    fn disconnect(&mut self, addr: SocketAddr);
}
