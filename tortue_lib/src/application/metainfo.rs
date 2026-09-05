use anyhow::Result;

use crate::domain::torrent::Metainfo;

pub async fn metainfo(torrent_file: &[u8]) -> Result<Metainfo> {
    let metainfo = Metainfo::try_from(torrent_file)?;
    Ok(metainfo)
}
