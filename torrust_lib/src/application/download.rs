use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::info;

use crate::adapters::disk_storage::DiskStorage;
use crate::adapters::pool_io::PoolIO;
use crate::adapters::tracker_io::TrackerIO;
use crate::domain::peer::PeerId;
use crate::domain::torrent::Metainfo;
use crate::domain::tracker::Node;

pub async fn download(torrent_file: &[u8], output_dir: PathBuf) -> Result<()> {
    let metainfo = Arc::new(Metainfo::try_from(torrent_file)?);
    let node = Node {
        id: PeerId::generate("TR", "0.1.0"),
        port: 1234,
    };

    info!(name = metainfo.name, "start_download");
    let (peers_tx, peers_rx) = mpsc::channel(128);

    for url in &metainfo.announce {
        let tracker = TrackerIO::new(url, Arc::clone(&metainfo), node, peers_tx.clone());
        if let Ok(mut runner) = tracker {
            tokio::spawn(async move { runner.run().await });
        }
    }

    let storage = DiskStorage::new(&metainfo, output_dir).await?;
    let mut pool_runner = PoolIO::new(Arc::clone(&metainfo), node.id, peers_rx, storage);
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
