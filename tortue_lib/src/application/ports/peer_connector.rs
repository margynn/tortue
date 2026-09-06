use std::net::SocketAddr;

use tokio::sync::mpsc;

use crate::domain::{message::Message, peer::PeerEvent};

/// Factory that spawns one peer connection task per `connect()` call.
/// Implementations hold shared config (credentials, transport settings)
/// and produce independent per-peer tasks.
pub trait PeerConnector: Send + Sync + 'static {
    fn connect(
        &self,
        addr: SocketAddr,
        cmd_rx: mpsc::Receiver<Message>,
        events_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    );
}
