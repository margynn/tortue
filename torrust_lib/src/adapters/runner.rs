use std::net::SocketAddr;

use tokio::sync::mpsc;
use tokio::time::Instant;

use super::tracker_client::TrackerClient;
use super::tracker_runner::TrackerRunner;
use crate::adapters::pool_runner::PoolRunner;
use crate::domain::torrent::Metainfo;
use crate::domain::tracker::Node;

pub async fn run(metainfo: Metainfo, node: Node) {
    let (peers_tx, peers_rx) = mpsc::channel(128);
    // let (piece_tx, piece_rx) = mpsc::channel(32);

    for url in &metainfo.announce {
        let Ok(client) = TrackerClient::new(url) else { continue };
        let mut runner = TrackerRunner::new(client, metainfo.clone(), node, peers_tx.clone());
        tokio::spawn(async move { runner.run().await });
    }

    let mut pool_runner = PoolRunner::new(metainfo.clone(), node.id, peers_rx);
    tokio::spawn(async move { pool_runner.run().await });

    // // Storage
    // tokio::spawn(storage_task(metainfo.clone(), piece_rx));
}
