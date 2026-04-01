use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout};

use super::{Error, PeerAddr, PeerId, bitfield};
use crate::peer::swarm::{PeerCommand, PeerEvent};

#[derive(Debug)]
pub struct PeerClient {
    // Peer info
    peer_id: PeerId,
    peer_addr: PeerAddr,
    peer_state: PeerState,

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

impl PeerState {
    fn new(pieces: usize) -> Self {
        Self {
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            bitfield: bitfield::Bitfield::new(pieces),
        }
    }

    fn reset(&mut self, pieces: usize) {
        *self = Self::new(pieces);
    }

    fn apply(&mut self, msg: &Message) {
        match msg {
            Message::Choke => self.peer_choking = true,
            Message::Unchoke => self.peer_choking = false,
            Message::Interested => self.peer_interested = true,
            Message::NotInterested => self.peer_interested = false,
            Message::Bitfield(bits) => {
                if let Ok(bitfield) =
                    bitfield::Bitfield::try_from(bits.as_ref())
                {
                    self.bitfield = bitfield;
                }
            },
            Message::Have(piece) => {
                let _ = self.bitfield.set_bit(*piece as usize);
            },
            Message::KeepAlive | Message::Piece { .. } => {},
            _ => {},
        }
    }
}

#[derive(Debug, Clone)]
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
            Message::KeepAlive => buf.extend_from_slice(&0u32.to_be_bytes()),
            Message::Choke => buf.extend_from_slice(&[0, 0, 0, 1, 0]),
            Message::Unchoke => buf.extend_from_slice(&[0, 0, 0, 1, 1]),
            Message::Interested => buf.extend_from_slice(&[0, 0, 0, 1, 2]),
            Message::NotInterested => buf.extend_from_slice(&[0, 0, 0, 1, 3]),
            Message::Have(piece) => {
                buf.extend_from_slice(&5u32.to_be_bytes());
                buf.push(4);
                buf.extend_from_slice(&piece.to_be_bytes());
            },
            Message::Bitfield(bits) => {
                let len = 1u32
                    .checked_add(bits.len() as u32)
                    .ok_or(Error::InvalidMessage)?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.push(5);
                buf.extend_from_slice(bits);
            },
            Message::Request { index, begin, length } => {
                buf.extend_from_slice(&13u32.to_be_bytes());
                buf.push(6);
                buf.extend_from_slice(&index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());
            },
            Message::Piece { index, begin, block } => {
                let len = 9u32
                    .checked_add(block.len() as u32)
                    .ok_or(Error::InvalidMessage)?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.push(7);
                buf.extend_from_slice(&index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(block);
            },
            Message::Cancel { index, begin, length } => {
                buf.extend_from_slice(&13u32.to_be_bytes());
                buf.push(8);
                buf.extend_from_slice(&index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());
            },
        }

        Ok(buf)
    }

    pub fn decode(data: &[u8]) -> Result<Message, Error> {
        if data.is_empty() {
            return Ok(Message::KeepAlive);
        }

        let msg_id = data[0];
        let payload = &data[1..];

        match msg_id {
            0 => Ok(Message::Choke),
            1 => Ok(Message::Unchoke),
            2 => Ok(Message::Interested),
            3 => Ok(Message::NotInterested),
            4 => {
                if payload.len() != 4 {
                    return Err(Error::InvalidMessage);
                }
                let piece = u32::from_be_bytes(
                    payload.try_into().map_err(|_| Error::InvalidMessage)?,
                );
                Ok(Message::Have(piece))
            },
            5 => Ok(Message::Bitfield(payload.to_vec())),
            6 => {
                if payload.len() != 12 {
                    return Err(Error::InvalidMessage);
                }
                let index = u32::from_be_bytes(
                    payload[0..4]
                        .try_into()
                        .map_err(|_| Error::InvalidMessage)?,
                );
                let begin = u32::from_be_bytes(
                    payload[4..8]
                        .try_into()
                        .map_err(|_| Error::InvalidMessage)?,
                );
                let length = u32::from_be_bytes(
                    payload[8..12]
                        .try_into()
                        .map_err(|_| Error::InvalidMessage)?,
                );
                Ok(Message::Request { index, begin, length })
            },
            7 => {
                if payload.len() < 8 {
                    return Err(Error::InvalidMessage);
                }
                let index = u32::from_be_bytes(
                    payload[0..4]
                        .try_into()
                        .map_err(|_| Error::InvalidMessage)?,
                );
                let begin = u32::from_be_bytes(
                    payload[4..8]
                        .try_into()
                        .map_err(|_| Error::InvalidMessage)?,
                );
                let block = payload[8..].to_vec();
                Ok(Message::Piece { index, begin, block })
            },
            8 => {
                if payload.len() != 12 {
                    return Err(Error::InvalidMessage);
                }
                let index = u32::from_be_bytes(
                    payload[0..4]
                        .try_into()
                        .map_err(|_| Error::InvalidMessage)?,
                );
                let begin = u32::from_be_bytes(
                    payload[4..8]
                        .try_into()
                        .map_err(|_| Error::InvalidMessage)?,
                );
                let length = u32::from_be_bytes(
                    payload[8..12]
                        .try_into()
                        .map_err(|_| Error::InvalidMessage)?,
                );
                Ok(Message::Cancel { index, begin, length })
            },
            _ => Err(Error::InvalidMessage),
        }
    }
}

enum ConnectionState {
    Disconnected {
        next_retry_at: Instant,
        backoff: Duration,
    },
    Connected(TcpStream),
}

impl PeerClient {
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
    const RECONNECT_DELAY: Duration = Duration::from_secs(2);
    const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);
    const MAX_MESSAGE_SIZE: u32 = 1 << 20; // 1MB safety cap

    pub fn new(
        peer_addr: PeerAddr,
        torrent_info_hash: [u8; 20],
        client_id: PeerId,
        pieces: usize,
    ) -> Self {
        Self {
            peer_id: PeerId::new([0; 20]),
            peer_addr,
            peer_state: PeerState::new(pieces),
            torrent_info_hash,
            client_id,
            pieces,
        }
    }

    pub async fn run(
        mut self,
        mut cmd_rx: mpsc::Receiver<PeerCommand>,
        event_tx: mpsc::Sender<PeerEvent>,
    ) {
        let mut state = ConnectionState::Disconnected {
            next_retry_at: Instant::now(),
            backoff: Self::RECONNECT_DELAY,
        };

        loop {
            match state {
                ConnectionState::Disconnected { next_retry_at, backoff } => {
                    tokio::select! {
                        cmd = cmd_rx.recv() => {
                            match cmd {
                                Some(PeerCommand::Shutdown) | None => break,
                                Some(_) => {
                                    state = ConnectionState::Disconnected {
                                        next_retry_at,
                                        backoff,
                                    };
                                }
                            }
                        }

                        _ = tokio::time::sleep_until(next_retry_at) => {
                            match self.connect().await {
                                Ok(conn) => {
                                    self.peer_state.reset(self.pieces);
                                    let _ = event_tx
                                        .send(PeerEvent::Connected(self.peer_addr))
                                        .await;
                                    state = ConnectionState::Connected(conn);
                                }
                                Err(_) => {
                                    let next_backoff = (backoff * 2).min(Self::MAX_RECONNECT_DELAY);
                                    state = ConnectionState::Disconnected {
                                        next_retry_at: Instant::now() + backoff,
                                        backoff: next_backoff,
                                    };
                                }
                            }
                        }
                    }
                },

                ConnectionState::Connected(mut conn) => {
                    tokio::select! {
                        cmd = cmd_rx.recv() => {
                            match cmd {
                                Some(PeerCommand::Shutdown) | None => {
                                    let _ = event_tx
                                        .send(PeerEvent::Disconnected(self.peer_addr))
                                        .await;
                                    break;
                                }
                                Some(_) => {
                                    // Map concrete commands to wire messages here when needed.
                                    // For now, unknown/non-shutdown commands are ignored.
                                    state = ConnectionState::Connected(conn);
                                }
                            }
                        }

                        res = self.read_message(&mut conn) => {
                            match res {
                                Ok(msg) => {
                                    self.peer_state.apply(&msg);

                                    let _ = event_tx
                                        .send(PeerEvent::Message(self.peer_addr, msg))
                                        .await;

                                    state = ConnectionState::Connected(conn);
                                }
                                Err(_) => {
                                    let _ = event_tx
                                        .send(PeerEvent::Disconnected(self.peer_addr))
                                        .await;

                                    state = ConnectionState::Disconnected {
                                        next_retry_at: Instant::now() + Self::RECONNECT_DELAY,
                                        backoff: Self::RECONNECT_DELAY,
                                    };
                                }
                            }
                        }
                    }
                },
            }
        }
    }

    async fn connect(&mut self) -> Result<TcpStream, Error> {
        let addr =
            std::net::SocketAddr::new(self.peer_addr.0, self.peer_addr.1);

        let mut stream =
            timeout(Self::CONNECT_TIMEOUT, TcpStream::connect(addr))
                .await
                .map_err(|_| Error::Timeout)?
                .map_err(Error::Io)?;

        let outbound = Handshake::new(self.torrent_info_hash, self.client_id);
        timeout(Self::CONNECT_TIMEOUT, stream.write_all(&outbound.encode()))
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(Error::Io)?;

        let mut buf = [0u8; Handshake::HANDSHAKE_LEN];
        timeout(Self::CONNECT_TIMEOUT, stream.read_exact(&mut buf))
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(Error::Io)?;

        let inbound = Handshake::decode(&buf)?;

        if inbound.info_hash != self.torrent_info_hash {
            return Err(Error::InfoHashMismatch);
        }

        self.peer_id = inbound.peer_id;

        Ok(stream)
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
        if length > Self::MAX_MESSAGE_SIZE {
            return Err(Error::InvalidMessage);
        }

        let mut payload = vec![0u8; length as usize];
        stream.read_exact(&mut payload).await.map_err(Error::Io)?;

        Message::decode(&payload)
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

struct Handshake {
    info_hash: [u8; 20],
    peer_id: PeerId,
}

impl Handshake {
    const PSTR: &[u8; 19] = b"BitTorrent protocol";
    const HANDSHAKE_LEN: usize = 68;
    const RESERVED_LEN: usize = 8;

    fn new(info_hash: [u8; 20], peer_id: PeerId) -> Self {
        Self { info_hash, peer_id }
    }

    fn encode(&self) -> [u8; Self::HANDSHAKE_LEN] {
        let mut out = [0u8; Self::HANDSHAKE_LEN];
        out[0] = Self::PSTR.len() as u8;
        out[1..20].copy_from_slice(Self::PSTR);
        out[20..28].copy_from_slice(&[0; Self::RESERVED_LEN]);
        out[28..48].copy_from_slice(&self.info_hash);
        out[48..68].copy_from_slice(self.peer_id.as_ref());
        out
    }

    fn decode(buf: &[u8]) -> Result<Self, Error> {
        if buf.len() != Self::HANDSHAKE_LEN {
            return Err(Error::InvalidHandshake("invalid handshake length"));
        }

        let pstrlen = buf[0] as usize;
        if pstrlen != Self::PSTR.len() {
            return Err(Error::InvalidHandshake(
                "invalid protocol string length",
            ));
        }
        if &buf[1..20] != Self::PSTR {
            return Err(Error::InvalidHandshake("invalid protocol string"));
        }

        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&buf[28..48]);

        let mut peer_id_bytes = [0u8; 20];
        peer_id_bytes.copy_from_slice(&buf[48..68]);

        Ok(Self {
            info_hash,
            peer_id: PeerId::new(peer_id_bytes),
        })
    }
}
