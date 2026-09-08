use std::{net::SocketAddr, sync::Arc};

use super::errors::{Error, Result};
use crate::{Metainfo, adapters::metadata_io::MetadataIO, domain::magnet::MagnetLink};

pub async fn fetch_metadata(magnet: MagnetLink) -> Result<Arc<Metainfo>> {
    let peers: Vec<SocketAddr> = magnet.peers.iter().filter_map(|s| s.parse().ok()).collect();
    let raw_info = MetadataIO::run(magnet.info_hash, magnet.trackers, peers).await?;
    let metainfo = Metainfo::try_from(raw_info.as_slice()).map_err(Error::InvalidMetainfo)?;
    Ok(Arc::new(metainfo))
}
