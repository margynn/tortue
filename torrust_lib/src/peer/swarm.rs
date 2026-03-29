use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::timeout;

use super::client::PeerClient;
use super::{Error, Peer};
use crate::tracker::TrackerSession;
use crate::tracker::session::Node;

pub struct Swarm {
    torrent_info_hash: [u8; 20],
    node: Node,
    pieces: usize,
    peers_rx: mpsc::Receiver<(Vec<Peer>, Arc<TrackerSession>)>,
    peers: HashMap<Peer, PeerConnection>,
}

struct PeerConnection {
    session: Arc<TrackerSession>,
    client: PeerClient,
}

impl Swarm {
    pub fn new(
        torrent_info_hash: [u8; 20],
        node: Node,
        pieces: usize,
        peers_rx: mpsc::Receiver<(Vec<Peer>, Arc<TrackerSession>)>,
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
        while let Some((peers, session)) = self.peers_rx.recv().await {
            // todo: should run the connect in parallel instead
            let _ = self.connect(peers, session).await;
        }
    }

    async fn connect(
        &mut self,
        peers: Vec<Peer>,
        session: Arc<TrackerSession>,
    ) -> Result<(), Error> {
        let info_hash = self.torrent_info_hash;
        let local_peer_id = self.node.id;
        let pieces = self.pieces;
        let mut set = JoinSet::new();

        for peer in peers {
            let session = session.clone();
            set.spawn(async move {
                let res = timeout(
                    Duration::from_secs(5),
                    PeerClient::connect(
                        peer.clone(),
                        info_hash,
                        local_peer_id,
                        pieces,
                    ),
                )
                .await;

                match res {
                    Ok(Ok(client)) => Ok((peer, client, session)),
                    Ok(Err(err)) => Err((peer, err)),
                    Err(_) => Err((peer, Error::Timeout)),
                }
            });
        }

        while let Some(task) = set.join_next().await {
            match task {
                Ok(res) => match res {
                    Ok((peer, client, session)) => {
                        self.peers
                            .entry(peer)
                            .or_insert(PeerConnection { session, client });
                    },
                    // Error in connect
                    _ => continue,
                },
                // Error in task - timeout
                _ => continue,
            }
        }

        Ok(())
    }
}
