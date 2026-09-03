use anyhow::Result;
use tokio::sync::mpsc;
use tracing::info;

use crate::adapters::pool_io::PoolIO;
use crate::adapters::tracker_io::TrackerIO;
use crate::domain;
use crate::domain::peer::PeerId;
use crate::domain::tracker::Node;

pub async fn download(torrent_file: &[u8]) -> Result<()> {
    let metainfo = domain::torrent::decode(torrent_file)?;
    let node = Node {
        id: PeerId::generate("TR", "0.1.0"),
        port: 1234,
    };

    info!(name = metainfo.name, "start_download");
    let (peers_tx, peers_rx) = mpsc::channel(128);

    for url in &metainfo.announce {
        if let Ok(mut runner) = TrackerIO::new(url, metainfo.clone(), node, peers_tx.clone()) {
            tokio::spawn(async move { runner.run().await });
        }
    }

    let mut pool_runner = PoolIO::new(metainfo.clone(), node.id, peers_rx);
    tokio::spawn(async move { pool_runner.run().await });

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
