use std::{net::SocketAddr, sync::Arc, time::Duration};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, tcp::OwnedReadHalf},
    sync::mpsc,
    task::JoinHandle,
    time::timeout,
};

use crate::{
    application::ports::peer_connector::{PeerConnector, PeerEvent},
    domain::{
        message::Message,
        peer::PeerId,
        torrent::{InfoHash, Metainfo},
    },
};

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
}

type Result<T> = std::result::Result<T, Error>;

pub struct TcpPeerConnector {
    client_id: PeerId,
    metainfo: Arc<Metainfo>,
}

impl TcpPeerConnector {
    pub fn new(client_id: PeerId, metainfo: Arc<Metainfo>) -> Self {
        Self {
            client_id,
            metainfo,
        }
    }
}

impl PeerConnector for TcpPeerConnector {
    fn connect(
        &self,
        addr: SocketAddr,
        cmd_rx: mpsc::Receiver<Message>,
        evt_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    ) {
        let mut runner = PeerIO::new(addr, self.client_id, Arc::clone(&self.metainfo));
        tokio::spawn(async move { runner.run(cmd_rx, evt_tx).await });
    }
}

pub struct PeerIO {
    client_id: PeerId,
    peer_addr: SocketAddr,
    metainfo: Arc<Metainfo>,
}

impl PeerIO {
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
    const RECONNECT_DELAY: Duration = Duration::from_secs(4);
    const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(90);
    const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(120);
    const READ_TIMEOUT: Duration = Duration::from_secs(30);
    const MAX_RECONNECTION: usize = 10;

    fn new(peer_addr: SocketAddr, client_id: PeerId, metainfo: Arc<Metainfo>) -> Self {
        Self {
            client_id,
            peer_addr,
            metainfo,
        }
    }

    async fn run(
        &mut self,
        mut cmd_rx: mpsc::Receiver<Message>,
        evt_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    ) -> Result<()> {
        let mut keepalive = tokio::time::interval(Self::KEEPALIVE_INTERVAL);
        let mut reconnect_delay = Duration::ZERO;
        let mut reconnect_cpt = 0;

        'run: loop {
            if reconnect_cpt > Self::MAX_RECONNECTION {
                break;
            }
            reconnect_cpt += 1;
            let (tcp, peer_id) = self.connect_with_retry(reconnect_delay).await;

            let _ = evt_tx
                .send((self.peer_addr, PeerEvent::Connected(peer_id)))
                .await;

            let (reader, mut writer) = tcp.into_split();
            let mut read_task = self.spawn_reader(reader, evt_tx.clone());

            loop {
                tokio::select! {
                    cmd = cmd_rx.recv() => match cmd {
                        None => {
                            // Make sure to close reader task upon exit
                            read_task.abort();
                            break 'run
                        },
                        Some(msg) => {
                            if writer.write_all(&msg.encode()).await.is_err() {
                                break
                            }
                        },
                    },

                    _ = &mut read_task => break,

                    _ = keepalive.tick() => {
                        if writer.write_all(&Message::KeepAlive.encode()).await.is_err() {
                           break
                        }
                     },
                }
            }

            reconnect_delay = Self::RECONNECT_DELAY;
            read_task.abort()
        }

        // Sentinel: ensures Pool always receives Disconnected even on clean exit.
        let _ = evt_tx.send((self.peer_addr, PeerEvent::Disconnected)).await;
        Ok(())
    }

    fn spawn_reader(
        &self,
        mut reader: OwnedReadHalf,
        tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    ) -> JoinHandle<()> {
        let addr = self.peer_addr;

        tokio::spawn(async move {
            loop {
                let msg = match timeout(Self::READ_TIMEOUT, Message::read_from(&mut reader)).await {
                    Ok(Ok(msg)) => msg,
                    _ => return,
                };

                if tx
                    .send((addr, PeerEvent::MessageReceived(msg)))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        })
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

        let outbound = Handshake::new(self.metainfo.hash, self.client_id, false);
        timeout(Self::CONNECT_TIMEOUT, stream.write_all(&outbound.encode()))
            .await
            .map_err(|_| Error::Timeout)??;

        let mut buf = [0u8; Handshake::HANDSHAKE_LEN];
        timeout(
            Self::CONNECT_TIMEOUT,
            AsyncReadExt::read_exact(&mut stream, &mut buf),
        )
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
    pub fast_extension: bool,
}

impl Handshake {
    const PSTR: &[u8; 19] = b"BitTorrent protocol";
    const HANDSHAKE_LEN: usize = 68;
    const FAST_EXTENSION_MASK: u8 = 0b0000_0100;

    fn new(info_hash: InfoHash, peer_id: PeerId, fast_extension: bool) -> Self {
        Self {
            info_hash,
            peer_id,
            fast_extension,
        }
    }

    fn encode(&self) -> [u8; Self::HANDSHAKE_LEN] {
        let mut out = [0u8; Self::HANDSHAKE_LEN];
        out[0] = Self::PSTR.len() as u8;
        out[1..20].copy_from_slice(Self::PSTR);
        if self.fast_extension {
            out[27] |= Self::FAST_EXTENSION_MASK;
        }
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

        let fast_extension = (buf[27] & Self::FAST_EXTENSION_MASK) != 0;

        Ok(Handshake::new(
            InfoHash::from(hash_bytes),
            PeerId::new(peer_id_bytes),
            fast_extension,
        ))
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
                buf.extend_from_slice(&(*piece as u32).to_be_bytes());
            },
            Message::Bitfield(bits) => {
                buf.extend_from_slice(&(1 + bits.len() as u32).to_be_bytes());
                buf.push(5);
                buf.extend_from_slice(bits);
            },
            Message::Request {
                piece_index,
                piece_offset,
                piece_len,
            } => {
                buf.extend_from_slice(&13u32.to_be_bytes());
                buf.push(6);
                buf.extend_from_slice(&(*piece_index as u32).to_be_bytes());
                buf.extend_from_slice(&(*piece_offset as u32).to_be_bytes());
                buf.extend_from_slice(&(*piece_len as u32).to_be_bytes());
            },
            Message::Piece {
                piece_index,
                piece_offset,
                data,
            } => {
                buf.extend_from_slice(&(9 + data.len() as u32).to_be_bytes());
                buf.push(7);
                buf.extend_from_slice(&(*piece_index as u32).to_be_bytes());
                buf.extend_from_slice(&(*piece_offset as u32).to_be_bytes());
                buf.extend_from_slice(data);
            },
            Message::Cancel {
                piece_index,
                piece_offset,
                piece_len,
            } => {
                buf.extend_from_slice(&13u32.to_be_bytes());
                buf.push(8);
                buf.extend_from_slice(&(*piece_index as u32).to_be_bytes());
                buf.extend_from_slice(&(*piece_offset as u32).to_be_bytes());
                buf.extend_from_slice(&(*piece_len as u32).to_be_bytes());
            },
            Message::Unimplemented => {},
        }
        buf
    }

    async fn read_from<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self> {
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
                Ok(Message::Have(u32::from_be_bytes(
                    payload.try_into().map_err(|_| Error::InvalidMessage)?,
                ) as usize))
            },
            5 => Ok(Message::Bitfield(payload.to_vec())),
            6 => {
                if payload.len() != 12 {
                    return Err(Error::InvalidMessage);
                }
                Ok(Message::Request {
                    piece_index: u32::from_be_bytes(
                        payload[0..4]
                            .try_into()
                            .map_err(|_| Error::InvalidMessage)?,
                    ) as usize,
                    piece_offset: u32::from_be_bytes(
                        payload[4..8]
                            .try_into()
                            .map_err(|_| Error::InvalidMessage)?,
                    ) as usize,
                    piece_len: u32::from_be_bytes(
                        payload[8..12]
                            .try_into()
                            .map_err(|_| Error::InvalidMessage)?,
                    ) as usize,
                })
            },
            7 => {
                if payload.len() < 8 {
                    return Err(Error::InvalidMessage);
                }
                Ok(Message::Piece {
                    piece_index: u32::from_be_bytes(
                        payload[0..4]
                            .try_into()
                            .map_err(|_| Error::InvalidMessage)?,
                    ) as usize,
                    piece_offset: u32::from_be_bytes(
                        payload[4..8]
                            .try_into()
                            .map_err(|_| Error::InvalidMessage)?,
                    ) as usize,
                    data: payload[8..].to_vec(),
                })
            },
            8 => {
                if payload.len() != 12 {
                    return Err(Error::InvalidMessage);
                }
                Ok(Message::Cancel {
                    piece_index: u32::from_be_bytes(
                        payload[0..4]
                            .try_into()
                            .map_err(|_| Error::InvalidMessage)?,
                    ) as usize,
                    piece_offset: u32::from_be_bytes(
                        payload[4..8]
                            .try_into()
                            .map_err(|_| Error::InvalidMessage)?,
                    ) as usize,
                    piece_len: u32::from_be_bytes(
                        payload[8..12]
                            .try_into()
                            .map_err(|_| Error::InvalidMessage)?,
                    ) as usize,
                })
            },
            _ => Ok(Message::Unimplemented),
        }
    }
}
