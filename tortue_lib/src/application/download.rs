use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

use super::{
    errors::{Error, Result},
    ports::peer_source::PeerSource,
};
use crate::{
    adapters::{
        disk_storage::DiskStorage, peer_io::TcpPeerConnector, swarm_io::SwarmIO,
        tracker_io::TrackerIO,
    },
    application::magnet::fetch_metadata,
    domain::{
        magnet::MagnetLink,
        swarm::{SwarmSnapshot, SwarmStatus},
        torrent::Metainfo,
        tracker::{Node, SessionStats},
    },
};

pub struct Download {
    pub progress: watch::Receiver<SwarmSnapshot>,
    pub task: JoinHandle<Result<()>>,
}

pub async fn download(torrent_file: &[u8], output_dir: PathBuf) -> Result<Download> {
    let metainfo = Arc::new(
        Metainfo::try_from(torrent_file).map_err(|e| Error::InvalidTorrentFile(e.to_string()))?,
    );
    start_download(metainfo, output_dir).await
}

pub async fn download_magnet(magnet: &str, output_dir: PathBuf) -> Result<Download> {
    let magnet = MagnetLink::try_from(magnet)?;
    print!("magnet: {:#?}", magnet);
    let metainfo = fetch_metadata(magnet).await?;

    start_download(metainfo, output_dir).await
}

async fn start_download(metainfo: Arc<Metainfo>, output_dir: PathBuf) -> Result<Download> {
    let node = Node::new();
    let stats = Arc::new(Mutex::new(SessionStats {
        swarm_status: SwarmStatus::Active,
        uploaded: 0,
        downloaded: 0,
        left: metainfo.total_size() as usize,
    }));

    let (peers_tx, peers_rx) = mpsc::channel(128);
    for url in &metainfo.announce {
        if let Ok(source) = TrackerIO::new(url, metainfo.info_hash, node, stats.clone()) {
            let tx = peers_tx.clone();
            tokio::spawn(async move { PeerSource::run(source, tx).await });
        }
    }

    let initial = SwarmSnapshot::default();
    let (progress_tx, progress_rx) = watch::channel(initial);

    let connector =
        TcpPeerConnector::new(node.id, metainfo.info_hash, Some(metainfo.info_bytes.len()));
    let storage = DiskStorage::new(&metainfo, output_dir).await?;

    let mut coordinator = SwarmIO::new(
        Arc::clone(&metainfo),
        peers_rx,
        connector,
        storage,
        progress_tx,
        stats,
    );
    let task = tokio::spawn(async move {
        coordinator
            .run()
            .await
            .map_err(|e| Error::Failed(e.to_string()))
    });

    Ok(Download {
        progress: progress_rx,
        task,
    })
}
