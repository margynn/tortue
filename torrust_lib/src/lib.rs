mod bencode;
mod metainfo;
mod peer;
mod tracker;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::peer::PeerAddr;
use crate::tracker::session::Node;
use crate::tracker::{PeerId, TrackerSession};

pub async fn download(torrent_file: &[u8]) -> Result<()> {
    let metainfo = metainfo::decode(&torrent_file)?;
    let torrent_info_hash = metainfo.hash;
    let content_size = metainfo.size();
    let pieces = metainfo.pieces.len();
    let local_peer_id = PeerId::generate("TR", "0.1.0");
    let node = Node { id: local_peer_id, port: 1234 };
    let mut sessions = Vec::new();

    let (tx, rx) = mpsc::channel::<Vec<PeerAddr>>(1024);

    // Start all trackers sessions concurrently
    for endpoint in metainfo.trackers() {
        let session = match TrackerSession::new(
            &endpoint,
            torrent_info_hash,
            node,
            content_size,
            tx.clone(),
        ) {
            Ok(t) => t,
            _ => continue,
        };
        session.clone().start();
        sessions.push(session);
        println!("tracker: {endpoint}");
    }

    // Send the sessions to the swarm
    let swarm = peer::Swarm::new(torrent_info_hash, node, pieces, rx);
    swarm.start();
    println!("swarm started");
    shutdown_signal().await;
    Ok(())
}

async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = sigterm.recv() => {},
    }
}
