use std::sync::Arc;

use tokio::sync::mpsc;

use super::errors::{Error, Result};
use crate::{
    Metainfo,
    adapters::{metadata_io::MetadataIO, peer_io::TcpPeerConnector, tracker_io::TrackerIO},
    application::ports::peer_source::PeerSource,
    domain::{magnet::MagnetLink, peer::PeerId, tracker::Node},
};

pub async fn fetch_metadata(magnet: MagnetLink) -> Result<Arc<Metainfo>> {
    let node = Node {
        id: PeerId::generate("TT", "0.1.0"),
        port: 1234,
    };
    let (peers_tx, peers_rx) = mpsc::channel(128);
    for url in &magnet.trackers {
        if let Ok(source) = TrackerIO::new(url, magnet.info_hash, node) {
            let tx = peers_tx.clone();
            tokio::spawn(async move { PeerSource::run(source, tx).await });
        }
    }

    let connector = TcpPeerConnector::new(node.id, magnet.info_hash, None);
    let mut metadata_io = MetadataIO::new(magnet, peers_rx, connector);
    let raw_info = metadata_io.run().await?; // bloque jusqu'à Done
    let metainfo = Metainfo::try_from(raw_info.as_slice())
        .map_err(|e| Error::InvalidTorrentFile(e.to_string()))?;
    Ok(Arc::new(metainfo))
}
