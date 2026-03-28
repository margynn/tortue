pub mod bencode;
pub mod metainfo;
pub mod tracker;

use anyhow::Result;

use crate::metainfo::Mode;
use crate::tracker::{PeerId, TrackerSession};

pub async fn download(torrent_file: &[u8]) -> Result<()> {
    let metainfo = metainfo::decode(&torrent_file)?;
    let peer_id = PeerId::generate("TR", "0.1.0");
    let trackers = metainfo.trackers();

    println!("{:#?}", trackers);

    let left = match &metainfo.mode {
        Mode::Single { length } => *length as u64,
        Mode::Multiple { files } => files.iter().map(|f| f.length as u64).sum(),
    };

    for tracker in trackers {
        let mut session = match TrackerSession::new(
            &tracker,
            metainfo.hash,
            peer_id,
            4444,
            left,
        ) {
            Ok(t) => t,
            _ => continue,
        };
        let response = match session.start().await {
            Ok(r) => r,
            _ => continue,
        };
        println!("{response:#?}");
        break;
    }

    Ok(())
}
