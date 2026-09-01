mod adapters;
mod domain;

use anyhow::{Ok, Result};
use tracing::info;

use crate::adapters::runner;
use crate::domain::peer::PeerId;
use crate::domain::tracker::Node;

pub async fn download(torrent_file: &[u8]) -> Result<()> {
    let metainfo = domain::torrent::decode(torrent_file)?;
    let node = Node {
        id: PeerId::generate("TR", "0.1.0"),
        port: 1234,
    };
    info!(name = metainfo.name, "start_download");
    runner::run(metainfo, node).await; // TODO: return error + pass a storage implementation
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
