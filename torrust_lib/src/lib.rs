mod bencode;
mod metainfo;
mod peer;
mod tracker;

use anyhow::Result;

use crate::tracker::{PeerId, TrackerSession};

pub async fn download(torrent_file: &[u8]) -> Result<()> {
    let metainfo = metainfo::decode(&torrent_file)?;
    let peer_id = PeerId::generate("TR", "0.1.0");
    let trackers = metainfo.trackers();

    println!("{:#?}", trackers);

    for tracker in trackers {
        let session = match TrackerSession::new(
            &tracker,
            metainfo.hash,
            peer_id,
            4444,
            metainfo.size(),
        ) {
            Ok(t) => t,
            _ => continue,
        };
        let response = match session.announce_started().await {
            Ok(r) => r,
            _ => continue,
        };
        println!("{response:#?}");
        break;
    }

    Ok(())
}
