use std::collections::HashMap;

use tokio::sync::mpsc;

use super::client::PeerClient;
use crate::peer::PeerAddr;
use crate::tracker::session::Node;

pub struct Swarm {
    torrent_info_hash: [u8; 20],
    node: Node,
    pieces: usize,
    peers_rx: mpsc::Receiver<Vec<PeerAddr>>,
    peers: HashMap<PeerAddr, PeerEntry>,
}

struct PeerEntry {
    peer: PeerAddr,
    status: PeerStatus,
    failures: u32,
}

#[derive(Debug, Clone, Copy)]
enum PeerStatus {
    New,
    Connecting,
    Running,
}

impl Swarm {
    pub fn new(
        torrent_info_hash: [u8; 20],
        node: Node,
        pieces: usize,
        peers_rx: mpsc::Receiver<Vec<PeerAddr>>,
    ) -> Self {
        Self {
            torrent_info_hash,
            node,
            pieces,
            peers_rx,
            peers: HashMap::new(),
        }
    }

    pub fn start(mut self) {
        tokio::spawn(async move {
            self.run_swarm().await;
        });
    }

    async fn run_swarm(&mut self) {
        while let Some(peers) = self.peers_rx.recv().await {
            self.add_peers(peers);
            self.spawn_peers();
        }
    }

    fn add_peers(&mut self, peers: Vec<PeerAddr>) {
        for peer in peers {
            self.peers.entry(peer).or_insert(PeerEntry {
                peer,
                status: PeerStatus::New,
                failures: 0,
            });
        }
    }

    fn spawn_peers(&mut self) {
        let info_hash = self.torrent_info_hash;
        let local_peer_id = self.node.id;
        let pieces = self.pieces;

        for entry in self.peers.values_mut() {
            if matches!(entry.status, PeerStatus::New) {
                entry.status = PeerStatus::Connecting;

                let peer = entry.peer.clone();

                tokio::spawn(async move {
                    let client =
                        PeerClient::new(peer, info_hash, local_peer_id, pieces);

                    client.run().await;
                });
            }
        }
    }
}
