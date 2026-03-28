mod bencode;
mod metainfo;
mod peer;
mod tracker;

use anyhow::Result;

use crate::tracker::session::Node;
use crate::tracker::{PeerId, TrackerSession};

pub async fn download(torrent_file: &[u8]) -> Result<()> {
    let metainfo = metainfo::decode(&torrent_file)?;
    let torrent_info_hash = metainfo.hash;
    let content_size = metainfo.size();
    let pieces = metainfo.pieces.len();
    let local_peer_id = PeerId::generate("TR", "0.1.0");
    let node = Node { id: local_peer_id, port: 1234 };
    let mut sessions = Vec::new();

    // Start all trackers sessions concurrently
    for endpoint in metainfo.trackers() {
        let session = match TrackerSession::new(
            &endpoint,
            torrent_info_hash,
            node,
            content_size,
        ) {
            Ok(t) => t,
            _ => continue,
        };
        session.clone().start();
        sessions.push(session);
        println!("start tracker: {endpoint}");
    }

    // Send the sessions to the swarm
    let mut swarm = peer::Swarm::new(torrent_info_hash, node, pieces, sessions);

    //     let mut sw = peer::Swarm::new(torrent_info_hash, local_peer_id, pieces);
    //     sw.connect(response.peers).await?;
    //     println!("connected to swarm");

    Ok(())
}
