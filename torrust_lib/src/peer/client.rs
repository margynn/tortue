use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use super::handshake::Handshake;
use super::{Error, Peer, PeerId, bitfield};

#[derive(Debug)]
pub struct PeerClient {
    stream: tokio::net::TcpStream,
    peer: Peer,
    state: PeerState,
}

#[derive(Debug)]
struct PeerState {
    am_choking: bool,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
    bitfield: bitfield::Bitfield,
}

#[derive(Debug)]
pub enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Vec<u8>),
    Request { index: u32, begin: u32, length: u32 },
    Piece { index: u32, begin: u32, block: Vec<u8> },
    Cancel { index: u32, begin: u32, length: u32 },
}

impl Message {
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut buf: Vec<u8> = Vec::new();
        match self {
            Message::Choke => buf.extend([0, 0, 0, 1, 0]),
            Message::Unchoke => buf.extend([0, 0, 0, 1, 1]),
            Message::Interested => buf.extend([0, 0, 0, 1, 2]),
            Message::NotInterested => buf.extend([0, 0, 0, 1, 3]),
            _ => unimplemented!(),
        };
        Ok(buf)
    }
}

pub fn decode(data: &[u8]) -> Result<Message, Error> {
    if data.len() == 0 {
        return Ok(Message::KeepAlive);
    }
    let msg_id = data[0];
    let data = &data[1..];
    match msg_id {
        0 => Ok(Message::Choke),
        1 => Ok(Message::Unchoke),
        2 => Ok(Message::Interested),
        3 => Ok(Message::NotInterested),
        4 => {
            if data.len() != 4 {
                return Err(Error::InvalidMessage);
            }
            let piece = u32::from_be_bytes(data.try_into().unwrap());
            Ok(Message::Have(piece))
        },
        5 => Ok(Message::Bitfield(data.to_vec())),
        7 => {
            if data.len() < 8 {
                return Err(Error::InvalidMessage);
            }

            let index = u32::from_be_bytes(data[0..4].try_into().unwrap());
            let begin = u32::from_be_bytes(data[4..8].try_into().unwrap());
            let block = data[8..].to_vec();

            Ok(Message::Piece { index, begin, block })
        },
        _ => Err(Error::InvalidMessage),
    }
}

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

        let state = PeerState {
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            bitfield: bitfield::Bitfield::new(pieces),
        };

        Ok(Self { stream, peer, state })
    }

    pub async fn run(mut self) -> Result<(), Error> {
        loop {
            let msg = self.read_message().await?;

            match msg {
                Message::Choke => {
                    self.state.peer_choking = true;
                },

                Message::Unchoke => {
                    self.state.peer_choking = false;
                },

                Message::Interested => {
                    self.state.peer_interested = true;
                },

                Message::NotInterested => {
                    self.state.peer_interested = false;
                },

                Message::Bitfield(bits) => {
                    self.state.bitfield =
                        bitfield::Bitfield::try_from(bits.as_ref()).unwrap();
                },

                Message::Have(piece) => {
                    let _ = self.state.bitfield.set_bit(piece as usize);
                },

                _ => {
                    // ignore for now
                },
            }
        }
    }

    async fn read_message(&mut self) -> Result<Message, Error> {
        let mut len_buf = [0u8; 4];

        self.stream.read_exact(&mut len_buf).await.map_err(Error::Io)?;
        let length = u32::from_be_bytes(len_buf);
        if length == 0 {
            return decode(&[0; 0]);
        }

        let mut payload = vec![0u8; length as usize];
        self.stream.read_exact(&mut payload).await.map_err(Error::Io)?;
        decode(payload.as_ref())
    }

    // async fn send(&mut self, msg: Message) -> Result<(), Error> {
    //     self.stream.write_all(msg.encode()?.as_ref()).await?;
    //     Ok(())
    // }
}
