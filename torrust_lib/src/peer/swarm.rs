use std::collections::HashMap;

use tokio::sync::mpsc;

use super::client::PeerClient;
use crate::metainfo::Metainfo;
use crate::peer::PeerAddr;
use crate::peer::client::Message;
use crate::pieces::PieceManager;
use crate::tracker::session::Node;

pub struct Swarm {
    metainfo: Metainfo,
    piece_manager: PieceManager,
    node: Node, // todo rename
    peers_rx: mpsc::Receiver<Vec<PeerAddr>>,
    peers_cmd: HashMap<PeerAddr, mpsc::Sender<PeerCommand>>,
    peer_events_rx: mpsc::Receiver<PeerEvent>,
    peer_events_tx: mpsc::Sender<PeerEvent>,
}

#[derive(Debug)]
pub enum PeerEvent {
    Connected(PeerAddr),
    Disconnected(PeerAddr),
    Message(PeerAddr, Message),
}

#[derive(Debug)]
pub enum PeerCommand {
    Shutdown,
    Cancel,
    Interested,
    NotInterested,
    Request { index: u32, begin: u32, length: u32 },
}

impl Swarm {
    const PEER_CMD_CHAN_SIZE: usize = 32;
    const SWARM_EVENT_CHAN_SIZE: usize = 256;

    pub fn new(
        metainfo: Metainfo,
        piece_manager: PieceManager,
        node: Node,
        peers_rx: mpsc::Receiver<Vec<PeerAddr>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(Self::SWARM_EVENT_CHAN_SIZE);
        Self {
            metainfo,
            piece_manager,
            node,
            peers_rx,
            peers_cmd: HashMap::new(),
            peer_events_rx: rx,
            peer_events_tx: tx,
        }
    }

    pub fn start(mut self) {
        tokio::spawn(async move {
            self.run_swarm().await;
        });
    }

    async fn run_swarm(&mut self) {
        loop {
            tokio::select! {
                Some(peers) = self.peers_rx.recv() => {
                    for peer_addr in peers {
                        self.spawn_peer(peer_addr);
                    }
                }

                Some(event) = self.peer_events_rx.recv() => {
                    self.handle_event(event);
                }
            }
        }
    }

    fn handle_event(&mut self, evt: PeerEvent) {
        match evt {
            PeerEvent::Connected(p) => println!("connected: {p:#?}"),
            PeerEvent::Disconnected(p) => {
                self.peers_cmd.remove(&p);
            },
            PeerEvent::Message(p, msg) => {
                println!("msg: {p:#?} - {msg:#?}");

                // Leecher: (I am downloading data from peers)
                // Bitfield / Have -> send interrested if only we need the pieces
                // Chocke -> cancel requests / messages / inflights
                // Unchocke -> send request for the missing pieces
                // Piece -> store the piece

                // Seeder: (I am uploading data to peers)
                // Interrested -> send unchoke
                // NotInterrested -> send chocke
                // Request -> send the piece
            },
        }
    }

    fn spawn_peer(&mut self, peer_addr: PeerAddr) {
        if self.peers_cmd.contains_key(&peer_addr) {
            return;
        }

        let metainfo = self.metainfo.clone();
        let client_id = self.node.id;
        let (cmd_tx, cmd_rx) = mpsc::channel(Self::PEER_CMD_CHAN_SIZE);
        let event_tx = self.peer_events_tx.clone();
        self.peers_cmd.insert(peer_addr, cmd_tx);

        tokio::spawn(async move {
            PeerClient::new(peer_addr, client_id, metainfo)
                .run(cmd_rx, event_tx)
                .await;
        });
    }
}
