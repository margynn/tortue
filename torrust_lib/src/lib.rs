mod bencode;
mod metainfo;
mod peer;
mod tracker;

use anyhow::Result;

use crate::tracker::{PeerId, TrackerSession};

pub async fn download(torrent_file: &[u8]) -> Result<()> {
    let metainfo = metainfo::decode(&torrent_file)?;
    let torrent_info_hash = metainfo.hash;
    let trackers = metainfo.trackers();
    let pieces = metainfo.pieces.len();
    let local_peer_id = PeerId::generate("TR", "0.1.0");

    println!("{:#?}", trackers);

    for tracker in trackers {
        let session = match TrackerSession::new(
            &tracker,
            torrent_info_hash,
            local_peer_id,
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

        for peer in response.peers {
            let peer_client = peer::PeerClient::connect(
                peer,
                torrent_info_hash,
                local_peer_id,
                pieces,
            )
            .await?;

            println!("{peer_client:#?}");
        }
        break;
    }

    Ok(())
}
