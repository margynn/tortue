use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::{Error, PeerId};

const PSTR: &[u8; 19] = b"BitTorrent protocol";
const HANDSHAKE_LEN: usize = 68;
const RESERVED_LEN: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Handshake {
    pub reserved: [u8; RESERVED_LEN],
    pub info_hash: [u8; 20],
    pub peer_id: PeerId,
}

impl Handshake {
    #[must_use]
    pub fn new(info_hash: [u8; 20], peer_id: PeerId) -> Self {
        Self {
            reserved: [0; RESERVED_LEN],
            info_hash,
            peer_id,
        }
    }

    #[must_use]
    fn encode(&self) -> [u8; HANDSHAKE_LEN] {
        let mut out = [0u8; HANDSHAKE_LEN];
        out[0] = PSTR.len() as u8;
        out[1..20].copy_from_slice(PSTR);
        out[20..28].copy_from_slice(&self.reserved);
        out[28..48].copy_from_slice(&self.info_hash);
        out[48..68].copy_from_slice(self.peer_id.as_ref());
        out
    }

    pub async fn write_to(&self, stream: &mut TcpStream) -> Result<(), Error> {
        stream.write_all(&self.encode()).await?;
        Ok(())
    }

    pub async fn read_from(stream: &mut TcpStream) -> Result<Self, Error> {
        let mut buf = [0u8; HANDSHAKE_LEN];
        stream.read_exact(&mut buf).await?;

        let pstrlen = buf[0] as usize;
        if pstrlen != PSTR.len() {
            return Err(Error::InvalidHandshake(
                "invalid protocol string length",
            ));
        }

        if &buf[1..20] != PSTR {
            return Err(Error::InvalidHandshake("invalid protocol string"));
        }

        let mut reserved = [0u8; RESERVED_LEN];
        reserved.copy_from_slice(&buf[20..28]);

        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&buf[28..48]);

        let mut peer_id_bytes = [0u8; 20];
        peer_id_bytes.copy_from_slice(&buf[48..68]);

        Ok(Self {
            reserved,
            info_hash,
            peer_id: PeerId::new(peer_id_bytes),
        })
    }
}
