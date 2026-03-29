use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;

use super::handshake::Handshake;
use super::{Error, PeerAddr, PeerId, bitfield};
use crate::peer::swarm::{PeerCommand, PeerEvent};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const RECONNECT_DELAY: Duration = Duration::from_secs(2);
const MAX_MESSAGE_SIZE: u32 = 1 << 20; // 1MB safety cap

#[derive(Debug)]
pub struct PeerClient {
    // Peer info
    peer_id: PeerId,
    peer_addr: PeerAddr,
    peer_state: PeerState,

    // Control
    cmd_rx: mpsc::Receiver<PeerCommand>,
    event_tx: mpsc::Sender<PeerEvent>,

    // Torrent info
    torrent_info_hash: [u8; 20],
    client_id: PeerId,
    pieces: usize,
}

#[derive(Debug)]
struct PeerState {
    am_choking: bool,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
    bitfield: bitfield::Bitfield,
}

struct Connection {
    stream: TcpStream,
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
        let mut buf = Vec::new();

        match self {
            Message::Choke => buf.extend([0, 0, 0, 1, 0]),
            Message::Unchoke => buf.extend([0, 0, 0, 1, 1]),
            Message::Interested => buf.extend([0, 0, 0, 1, 2]),
            Message::NotInterested => buf.extend([0, 0, 0, 1, 3]),
            _ => unimplemented!(),
        }

        Ok(buf)
    }
}

pub fn decode(data: &[u8]) -> Result<Message, Error> {
    if data.is_empty() {
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
    pub fn new(
        peer_addr: PeerAddr,
        cmd_rx: mpsc::Receiver<PeerCommand>,
        event_tx: mpsc::Sender<PeerEvent>,
        torrent_info_hash: [u8; 20],
        client_id: PeerId,
        pieces: usize,
    ) -> Self {
        Self {
            peer_id: PeerId::new([0; 20]),
            peer_addr,
            peer_state: PeerState {
                am_choking: true,
                am_interested: false,
                peer_choking: true,
                peer_interested: false,
                bitfield: bitfield::Bitfield::new(pieces),
            },
            cmd_rx,
            event_tx,
            torrent_info_hash,
            client_id,
            pieces,
        }
    }

    pub async fn run(mut self) {
        let mut backoff = RECONNECT_DELAY;

        loop {
            if let Ok(mut conn) = self.connect().await {
                backoff = RECONNECT_DELAY;
                let _ = self.run_connection(&mut conn).await;
            }

            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(3600));
        }
    }

    async fn connect(&mut self) -> Result<Connection, Error> {
        let addr =
            std::net::SocketAddr::new(self.peer_addr.0, self.peer_addr.1);

        let mut stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(Error::Io)?;

        // handshake (outbound)
        let outbound = Handshake::new(self.torrent_info_hash, self.client_id);
        timeout(CONNECT_TIMEOUT, outbound.write_to(&mut stream))
            .await
            .map_err(|_| Error::Timeout)??;

        // handshake (inbound)
        let inbound =
            timeout(CONNECT_TIMEOUT, Handshake::read_from(&mut stream))
                .await
                .map_err(|_| Error::Timeout)??;

        if inbound.info_hash != self.torrent_info_hash {
            return Err(Error::InfoHashMismatch);
        }

        self.peer_id = inbound.peer_id;

        Ok(Connection { stream })
    }

    async fn run_connection(
        &mut self,
        conn: &mut Connection,
    ) -> Result<(), Error> {
        // reset peer-specific state on new connection
        self.peer_state.peer_choking = true;
        self.peer_state.peer_interested = false;
        self.peer_state.bitfield = bitfield::Bitfield::new(self.pieces);

        // TODO: should send existing bitfield

        loop {
            let msg = self.read_message(&mut conn.stream).await?;
            match msg {
                Message::Choke => self.peer_state.peer_choking = true,
                Message::Unchoke => self.peer_state.peer_choking = false,
                Message::Interested => self.peer_state.peer_interested = true,
                Message::NotInterested => {
                    self.peer_state.peer_interested = false
                },
                Message::Bitfield(bits) => {
                    self.peer_state.bitfield =
                        bitfield::Bitfield::try_from(bits.as_ref()).unwrap();
                },
                Message::Have(piece) => {
                    let _ = self.peer_state.bitfield.set_bit(piece as usize);
                },
                Message::Piece { .. } => {
                    // TODO: forward to swarm / piece manager
                },
                Message::KeepAlive => {},

                _ => {},
            }
        }
    }

    async fn read_message(
        &mut self,
        stream: &mut TcpStream,
    ) -> Result<Message, Error> {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.map_err(Error::Io)?;
        let length = u32::from_be_bytes(len_buf);

        if length == 0 {
            return Ok(Message::KeepAlive);
        }
        if length > MAX_MESSAGE_SIZE {
            return Err(Error::InvalidMessage);
        }

        let mut payload = vec![0u8; length as usize];
        stream.read_exact(&mut payload).await.map_err(Error::Io)?;

        decode(&payload)
    }

    pub async fn send(
        &mut self,
        stream: &mut TcpStream,
        msg: Message,
    ) -> Result<(), Error> {
        let buf = msg.encode()?;
        stream.write_all(&buf).await.map_err(Error::Io)?;
        Ok(())
    }
}
