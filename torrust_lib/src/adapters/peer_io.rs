use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::domain::message::Message;
use crate::domain::peer::PeerId;
use crate::domain::torrent::{InfoHash, Metainfo};

#[derive(Debug)]
pub enum PeerEvent {
    Connected(PeerId),
    Disconnected,
    MessageReceived(Message),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("connection timed out")]
    Timeout,

    #[error("info hash mismatch")]
    InfoHashMismatch,

    #[error("invalid message")]
    InvalidMessage,

    #[error("invalid handshake: {0}")]
    InvalidHandshake(&'static str),

    #[error("message too large")]
    MessageTooLarge,

    #[error("peer pool disconnected")]
    PeerPoolGone,
}

type Result<T> = std::result::Result<T, Error>;

pub trait AsyncByteReader {
    fn read_exact<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = std::io::Result<()>> + 'a;
}

impl AsyncByteReader for TcpStream {
    fn read_exact<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = std::io::Result<()>> + 'a {
        async move { AsyncReadExt::read_exact(self, buf).await.map(|_| ()) }
    }
}

pub struct PeerIO {
    client_id: PeerId,
    peer_addr: SocketAddr,
    metainfo: Arc<Metainfo>,
    cmd_rx: mpsc::Receiver<Message>,
    tx: mpsc::Sender<PeerEvent>,
}

impl PeerIO {
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
    const RECONNECT_DELAY: Duration = Duration::from_secs(4);
    const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(90);

    pub fn new(
        peer_addr: SocketAddr,
        client_id: PeerId,
        metainfo: Arc<Metainfo>,
        cmd_rx: mpsc::Receiver<Message>,
        tx: mpsc::Sender<PeerEvent>,
    ) -> Self {
        Self {
            client_id,
            peer_addr,
            metainfo,
            cmd_rx,
            tx,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut reconnect_delay = Duration::ZERO;

        'run: loop {
            let (mut tcp, peer_id) = self.connect_with_retry(reconnect_delay).await;

            self.tx
                .send(PeerEvent::Connected(peer_id))
                .await
                .map_err(|_| Error::PeerPoolGone)?;

            reconnect_delay = 'session: loop {
                tokio::select! {
                    res = Message::read_from(&mut tcp) => match res {
                        Ok(msg) => {
                            self.tx
                                .send(PeerEvent::MessageReceived(msg))
                                .await
                                .map_err(|_| Error::PeerPoolGone)?;
                        },
                        Err(_) => {
                            let _ = self.tx.send(PeerEvent::Disconnected).await;
                            break 'session Self::RECONNECT_DELAY;
                        },
                    },

                    cmd = self.cmd_rx.recv() => match cmd {
                        None => break 'run,
                        Some(msg) => {
                            if tcp.write_all(&msg.encode()).await.is_err() {
                                let _ = self.tx.send(PeerEvent::Disconnected).await;
                                break 'session Self::RECONNECT_DELAY;
                            }
                        },
                    },
                }
            };
        }

        Ok(())
    }

    async fn connect_with_retry(&self, mut delay: Duration) -> (TcpStream, PeerId) {
        loop {
            tokio::time::sleep(delay).await;
            match self.connect().await {
                Ok(result) => return result,
                Err(e) => {
                    tracing::debug!(addr = %self.peer_addr, error = %e, "peer connection failed, retrying");
                    delay = (delay * 2).clamp(Self::RECONNECT_DELAY, Self::MAX_RECONNECT_DELAY);
                },
            }
        }
    }

    async fn connect(&self) -> Result<(TcpStream, PeerId)> {
        let mut stream = timeout(Self::CONNECT_TIMEOUT, TcpStream::connect(self.peer_addr))
            .await
            .map_err(|_| Error::Timeout)??;

        let outbound = Handshake::new(self.metainfo.hash, self.client_id);
        timeout(Self::CONNECT_TIMEOUT, stream.write_all(&outbound.encode()))
            .await
            .map_err(|_| Error::Timeout)??;

        let mut buf = [0u8; Handshake::HANDSHAKE_LEN];
        timeout(Self::CONNECT_TIMEOUT, AsyncReadExt::read_exact(&mut stream, &mut buf))
            .await
            .map_err(|_| Error::Timeout)??;

        let inbound = Handshake::decode(&buf)?;

        if inbound.info_hash != self.metainfo.hash {
            return Err(Error::InfoHashMismatch);
        }

        Ok((stream, inbound.peer_id))
    }
}

pub struct Handshake {
    pub info_hash: InfoHash,
    pub peer_id: PeerId,
}

impl Handshake {
    const PSTR: &[u8; 19] = b"BitTorrent protocol";
    const HANDSHAKE_LEN: usize = 68;

    fn new(info_hash: InfoHash, peer_id: PeerId) -> Self {
        Self { info_hash, peer_id }
    }

    fn encode(&self) -> [u8; Self::HANDSHAKE_LEN] {
        let mut out = [0u8; Self::HANDSHAKE_LEN];
        out[0] = Self::PSTR.len() as u8;
        out[1..20].copy_from_slice(Self::PSTR);
        // out[20..28] reserved bytes, already zero
        out[28..48].copy_from_slice(self.info_hash.as_ref());
        out[48..68].copy_from_slice(self.peer_id.as_ref());
        out
    }

    fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() != Self::HANDSHAKE_LEN {
            return Err(Error::InvalidHandshake("invalid handshake length"));
        }
        if buf[0] as usize != Self::PSTR.len() {
            return Err(Error::InvalidHandshake("invalid protocol string length"));
        }
        if &buf[1..20] != Self::PSTR {
            return Err(Error::InvalidHandshake("invalid protocol string"));
        }

        let mut hash_bytes = [0u8; 20];
        hash_bytes.copy_from_slice(&buf[28..48]);

        let mut peer_id_bytes = [0u8; 20];
        peer_id_bytes.copy_from_slice(&buf[48..68]);

        Ok(Handshake::new(InfoHash::from(hash_bytes), PeerId::new(peer_id_bytes)))
    }
}

impl Message {
    const MAX_MESSAGE_SIZE: usize = 1024 * 1024; // 1Mb

    fn encode(&self) -> Vec<u8> {
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
                buf.extend_from_slice(&(1 + bits.len() as u32).to_be_bytes());
                buf.push(5);
                buf.extend_from_slice(bits);
            },
            Message::Request { piece_index, piece_offset, piece_len } => {
                buf.extend_from_slice(&13u32.to_be_bytes());
                buf.push(6);
                buf.extend_from_slice(&piece_index.to_be_bytes());
                buf.extend_from_slice(&piece_offset.to_be_bytes());
                buf.extend_from_slice(&piece_len.to_be_bytes());
            },
            Message::Piece { piece_index, piece_offset, data } => {
                buf.extend_from_slice(&(9 + data.len() as u32).to_be_bytes());
                buf.push(7);
                buf.extend_from_slice(&piece_index.to_be_bytes());
                buf.extend_from_slice(&piece_offset.to_be_bytes());
                buf.extend_from_slice(data);
            },
            Message::Cancel { piece_index, piece_offset, piece_len } => {
                buf.extend_from_slice(&13u32.to_be_bytes());
                buf.push(8);
                buf.extend_from_slice(&piece_index.to_be_bytes());
                buf.extend_from_slice(&piece_offset.to_be_bytes());
                buf.extend_from_slice(&piece_len.to_be_bytes());
            },
        }
        buf
    }

    async fn read_from<R: AsyncByteReader>(reader: &mut R) -> Result<Self> {
        let mut header = [0u8; 4];
        reader.read_exact(&mut header).await?;

        let len = u32::from_be_bytes(header) as usize;
        if len > Self::MAX_MESSAGE_SIZE {
            return Err(Error::MessageTooLarge);
        }

        let mut payload = vec![0u8; len];
        reader.read_exact(&mut payload).await?;

        Self::decode(&payload)
    }

    fn decode(data: &[u8]) -> Result<Self> {
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
                Ok(Message::Have(usize::from_be_bytes(
                    payload.try_into().map_err(|_| Error::InvalidMessage)?,
                )))
            },
            5 => Ok(Message::Bitfield(payload.to_vec())),
            6 => {
                if payload.len() != 12 {
                    return Err(Error::InvalidMessage);
                }
                Ok(Message::Request {
                    piece_index: usize::from_be_bytes(
                        payload[0..4].try_into().map_err(|_| Error::InvalidMessage)?,
                    ),
                    piece_offset: usize::from_be_bytes(
                        payload[4..8].try_into().map_err(|_| Error::InvalidMessage)?,
                    ),
                    piece_len: usize::from_be_bytes(
                        payload[8..12].try_into().map_err(|_| Error::InvalidMessage)?,
                    ),
                })
            },
            7 => {
                if payload.len() < 8 {
                    return Err(Error::InvalidMessage);
                }
                Ok(Message::Piece {
                    piece_index: usize::from_be_bytes(
                        payload[0..4].try_into().map_err(|_| Error::InvalidMessage)?,
                    ),
                    piece_offset: usize::from_be_bytes(
                        payload[4..8].try_into().map_err(|_| Error::InvalidMessage)?,
                    ),
                    data: payload[8..].to_vec(),
                })
            },
            8 => {
                if payload.len() != 12 {
                    return Err(Error::InvalidMessage);
                }
                Ok(Message::Cancel {
                    piece_index: usize::from_be_bytes(
                        payload[0..4].try_into().map_err(|_| Error::InvalidMessage)?,
                    ),
                    piece_offset: usize::from_be_bytes(
                        payload[4..8].try_into().map_err(|_| Error::InvalidMessage)?,
                    ),
                    piece_len: usize::from_be_bytes(
                        payload[8..12].try_into().map_err(|_| Error::InvalidMessage)?,
                    ),
                })
            },
            _ => Err(Error::InvalidMessage),
        }
    }
}
