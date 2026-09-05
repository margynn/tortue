use std::{path::PathBuf, sync::Arc};

use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

use crate::{
    adapters::{
        disk_storage::DiskStorage, peer_io::TcpPeerConnector, pool_io::PoolIO,
        tracker_io::TrackerIO,
    },
    application::{errors::DownloadError, ports::peer_source::PeerSource},
    domain::{peer::PeerId, pool::PoolSnapshot, torrent::Metainfo, tracker::Node},
};

pub struct Download {
    pub progress: watch::Receiver<PoolSnapshot>,
    pub task: JoinHandle<Result<(), DownloadError>>,
}

pub async fn download(torrent_file: &[u8], output_dir: PathBuf) -> Result<Download, DownloadError> {
    let metainfo = Arc::new(
        Metainfo::try_from(torrent_file)
            .map_err(|e| DownloadError::InvalidTorrentFile(e.to_string()))?,
    );
    let node = Node {
        id: PeerId::generate("TR", "0.1.0"),
        port: 1234,
    };

    let (peers_tx, peers_rx) = mpsc::channel(128);
    for url in &metainfo.announce {
        if let Ok(source) = TrackerIO::new(url, Arc::clone(&metainfo), node) {
            let tx = peers_tx.clone();
            tokio::spawn(async move { PeerSource::run(source, tx).await });
        }
    }

    let initial = PoolSnapshot {
        blocks_total: 0,
        blocks_done: 0,
        blocks_in_flight: 0,
        peers: Vec::new(),
    };
    let (progress_tx, progress_rx) = watch::channel(initial);

    let connector = TcpPeerConnector::new(node.id, Arc::clone(&metainfo));
    let storage = DiskStorage::new(&metainfo, output_dir).await?;
    let mut pool = PoolIO::new(
        Arc::clone(&metainfo),
        peers_rx,
        connector,
        storage,
        progress_tx,
    );
    let task = tokio::spawn(async move {
        pool.run()
            .await
            .map_err(|e| DownloadError::Failed(e.to_string()))
    });

    Ok(Download {
        progress: progress_rx,
        task,
    })
}
