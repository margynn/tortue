use std::time::Duration;

use tokio::net::TcpStream;
use tokio::time::timeout;

use super::handshake::Handshake;
use super::{Error, Peer, PeerClient, PeerId};

impl PeerClient {
    pub async fn connect(
        peer: Peer,
        torrent_info_hash: [u8; 20],
        local_peer_id: PeerId,
        pieces: usize,
    ) -> Result<Self, Error> {
        let addr = std::net::SocketAddr::new(peer.ip, peer.port);
        let mut stream =
            timeout(Duration::from_secs(10), TcpStream::connect(addr))
                .await
                .map_err(|_| Error::Timeout)?
                .map_err(Error::Io)?;

        let outbound = Handshake::new(torrent_info_hash, local_peer_id);
        timeout(Duration::from_secs(10), outbound.write_to(&mut stream))
            .await
            .map_err(|_| Error::Timeout)??;

        let inbound =
            timeout(Duration::from_secs(10), Handshake::read_from(&mut stream))
                .await
                .map_err(|_| Error::Timeout)??;

        if inbound.info_hash != torrent_info_hash {
            return Err(Error::InfoHashMismatch);
        }

        if let Some(expected_peer_id) = peer.peer_id {
            if inbound.peer_id != expected_peer_id {
                return Err(Error::PeerIdMismatch);
            }
        }

        let state = super::PeerState {
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            bitfield: super::bitfield::Bitfield::new(pieces),
        };

        Ok(Self { stream, peer, state })
    }
}
