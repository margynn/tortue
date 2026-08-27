mod domain;
mod tracker;

use std::path::PathBuf;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::domain::pieces::PieceManager;
use crate::tracker::session::Node;
use crate::tracker::{PeerId, TrackerSession};

pub async fn download(torrent_file: &[u8]) -> Result<()> {
    let metainfo = domain::torrent::decode(torrent_file)?;
    let torrent_info_hash = metainfo.hash;
    let content_size = metainfo.size();
    let local_peer_id = PeerId::generate("TR", "0.1.0");
    let node = Node { id: local_peer_id, port: 1234 };
    let mut sessions = Vec::new();

    let (tx, rx) = mpsc::channel::<Vec<PeerAddr>>(1024);

    // Start all trackers sessions concurrently
    for endpoint in &metainfo.announce {
        let session = match TrackerSession::new(
            endpoint,
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

    // // Create piece manager
    // let path = PathBuf::from("./out");
    // let piece_manager = PieceManager::new(metainfo.clone(), path).await?;

    // // Send the sessions to the swarm
    // let swarm = peer::Swarm::new(metainfo.clone(), piece_manager, node, rx);
    // swarm.start();
    // println!("swarm started");
    // shutdown_signal().await;
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
