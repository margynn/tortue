use std::time::Duration;

use tokio::task::JoinSet;
use tokio::time::timeout;

use super::client::PeerClient;
use super::{Error, Peer, PeerId};

pub struct Swarm {
    info_hash: [u8; 20],
    peer_id: PeerId,
    pieces: usize,
    clients: Vec<PeerClient>,
}

impl Swarm {
    pub fn new(info_hash: [u8; 20], peer_id: PeerId, pieces: usize) -> Self {
        Self {
            info_hash,
            peer_id,
            pieces,
            clients: Vec::new(),
        }
    }

    pub async fn connect(&mut self, peers: Vec<Peer>) -> Result<(), Error> {
        let info_hash = self.info_hash;
        let peer_id = self.peer_id;
        let pieces = self.pieces;
        let mut set = JoinSet::new();

        for peer in peers {
            set.spawn(async move {
                let res = timeout(
                    Duration::from_secs(5),
                    PeerClient::connect(
                        peer.clone(),
                        info_hash,
                        peer_id,
                        pieces,
                    ),
                )
                .await;

                match res {
                    Ok(Ok(client)) => Ok(client),
                    Ok(Err(err)) => Err((peer, err)),
                    Err(_) => Err((peer, Error::Timeout)),
                }
            });
        }

        while let Some(joined) = set.join_next().await {
            match joined {
                Ok(Ok(client)) => {
                    self.clients.push(client);
                    println!("connection accepted!");
                },
                Ok(Err((peer, err))) => {
                    // expected peer-level failure: log, metric, blacklist, ignore, etc.
                    println!("peer {:#?} connect failed: {:?}", peer, err);
                },
                Err(join_err) => {
                    // task panicked or was cancelled: usually more serious
                    println!("connect task failed: {:?}", join_err);
                },
            }
        }

        Ok(())
    }
}
