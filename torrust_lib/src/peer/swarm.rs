use std::collections::HashMap;

use tokio::sync::mpsc;

use super::client::PeerClient;
use crate::peer::PeerAddr;
use crate::peer::bitfield::Bitfield;
use crate::tracker::session::Node;

pub struct Swarm {
    torrent_info_hash: [u8; 20],
    node: Node,
    pieces: usize,
    peers_rx: mpsc::Receiver<Vec<PeerAddr>>,
    peers_cmd: HashMap<PeerAddr, mpsc::Sender<PeerCommand>>,
    peer_events_rx: mpsc::Receiver<PeerEvent>,
    peer_events_tx: mpsc::Sender<PeerEvent>,
}

const PEER_CMD_CHAN_SIZE: usize = 32;
const SWARM_EVENT_CHAN_SIZE: usize = 256;

#[derive(Debug)]
pub enum PeerEvent {
    Connected(PeerAddr),
    Disconnected(PeerAddr),
    Bitfield(PeerAddr, Bitfield),
    Have(PeerAddr, u32),
    Unchoke(PeerAddr),
    Choke(PeerAddr),
    Piece {
        peer: PeerAddr,
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
}

#[derive(Debug)]
pub enum PeerCommand {
    Interested,
    NotInterested,
    Request { index: u32, begin: u32, length: u32 },
}

impl Swarm {
    pub fn new(
        torrent_info_hash: [u8; 20],
        node: Node,
        pieces: usize,
        peers_rx: mpsc::Receiver<Vec<PeerAddr>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(SWARM_EVENT_CHAN_SIZE);
        Self {
            torrent_info_hash,
            node,
            pieces,
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
                   self.spawn_peers(peers);
                }

                Some(event) = self.peer_events_rx.recv() => {
                    self.handle_event(event);
                }
            }
        }
    }

    fn handle_event(&mut self, event: PeerEvent) {
        todo!()
    }

    fn spawn_peers(&mut self, peers: Vec<PeerAddr>) {
        let info_hash = self.torrent_info_hash;
        let client_id = self.node.id;
        let pieces = self.pieces;

        for peer_addr in peers {
            if self.peers_cmd.contains_key(&peer_addr) {
                continue;
            }

            let (cmd_tx, cmd_rx) = mpsc::channel(PEER_CMD_CHAN_SIZE);
            let event_tx = self.peer_events_tx.clone();
            self.peers_cmd.insert(peer_addr, cmd_tx);

            tokio::spawn(async move {
                PeerClient::new(
                    peer_addr, cmd_rx, event_tx, info_hash, client_id, pieces,
                )
                .run()
                .await;
            });
        }
    }
}
