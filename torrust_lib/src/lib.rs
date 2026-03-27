pub mod bencode;
pub mod metainfo;
pub mod tracker;

use anyhow::Result;

use crate::{
    metainfo::Mode,
    tracker::{PeerId, TrackerEndpoint, TrackerSession},
};

pub async fn download(torrent_file: &[u8]) -> Result<()> {
    let metainfo = metainfo::decode(&torrent_file)?;
    let peer_id = PeerId::generate("TR", "0.1.0");

    println!("{:#?}", metainfo.announce);
    println!("{:#?}", metainfo.announce_list);

    let left = match &metainfo.mode {
        Mode::Single { length } => *length as u64,
        Mode::Multiple { files } => files.iter().map(|f| f.length as u64).sum(),
    };

    for tier in metainfo.announce_list {
        for tracker in tier {
            let endpoint = match TrackerEndpoint::parse(&tracker) {
                Ok(e) => e,
                _ => continue,
            };
            let mut session =
                match TrackerSession::new(endpoint, metainfo.hash, peer_id, 4444, left) {
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
    }

    Ok(())
}
