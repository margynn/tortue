use std::net::SocketAddr;
use std::time::Instant;

use tokio::sync::mpsc;

use crate::adapters::tracker_client::TrackerClient;
use crate::domain::torrent::Metainfo;
use crate::domain::tracker::Node;
use crate::domain::tracker::session::{Input, Output, TrackerSession};

pub struct TrackerRunner {
    client: TrackerClient,
    metainfo: Metainfo,
    node: Node,
    peers_tx: mpsc::Sender<Vec<SocketAddr>>,
    next_announce_at: Instant,
}

impl TrackerRunner {
    pub fn new(
        client: TrackerClient,
        metainfo: Metainfo,
        node: Node,
        peers_tx: mpsc::Sender<Vec<SocketAddr>>,
    ) -> Self {
        Self {
            client,
            metainfo,
            node,
            peers_tx,
            next_announce_at: Instant::now(),
        }
    }

    pub async fn run(&mut self) {
        let mut session = TrackerSession::new(self.metainfo.hash, self.node, self.metainfo.size());
        loop {
            let input = self.next_input().await;
            let outputs = session.step(input);
            if self.handle_outputs(&mut session, outputs).await {
                break;
            }
        }
    }

    // Attend le timer et retourne le prochain Input
    async fn next_input(&self) -> Input {
        todo!()
    }

    // Retourne true si le task doit s'arrêter
    async fn handle_outputs(&mut self, session: &mut TrackerSession, outputs: Vec<Output>) -> bool {
        let mut should_stop = false;
        for out in outputs {
            match out {
                Output::Announce(announce_request) => {
                    let Ok(resp) = self.client.announce(announce_request).await else { continue };
                },
                Output::ScheduleAnnounce(duration) => {
                    todo!()
                },
                Output::EmitPeers(socket_addrs) => {
                    let _ = self.peers_tx.send(socket_addrs).await;
                },
                Output::Stop => should_stop = true,
            }
        }
        should_stop
    }
}
