use std::{collections::HashMap, net::SocketAddr, time::Duration};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, tcp::OwnedReadHalf},
    sync::mpsc,
    task::JoinHandle,
    time::timeout,
};

use crate::{
    application::ports::peer_connector::PeerConnector,
    domain::{
        message::{Error as DecodeError, ExtensionHandshake, Message, UT_METADATA_EXT_ID},
        peer::{PeerEvent, PeerExtensions, PeerId},
        torrent::InfoHash,
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

    #[error("invalid handshake: {0}")]
    InvalidHandshake(&'static str),

    #[error("message too large")]
    MessageTooLarge,

    #[error("message decode: {0}")]
    MessageDecode(#[from] DecodeError),
}

type Result<T> = std::result::Result<T, Error>;

pub struct TcpPeerConnector {
    client_id: PeerId,
    peer_config: PeerConfig,
}

#[derive(Clone, Copy)]
struct PeerConfig {
    info_hash: InfoHash,
    metadata_size: Option<usize>,
}

impl TcpPeerConnector {
    pub fn new(client_id: PeerId, info_hash: InfoHash, metadata_size: Option<usize>) -> Self {
        Self {
            client_id,
            peer_config: PeerConfig {
                info_hash,
                metadata_size,
            },
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
        let mut runner = TcpPeerIO::new(addr, self.client_id, self.peer_config);
        tokio::spawn(async move { runner.run(cmd_rx, evt_tx).await });
    }
}

struct TcpPeerIO {
    client_id: PeerId,
    peer_addr: SocketAddr,
    config: PeerConfig,
}

impl TcpPeerIO {
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
    const RECONNECT_DELAY: Duration = Duration::from_secs(4);
    const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(90);
    const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(120);
    const READ_TIMEOUT: Duration = Duration::from_secs(30);
    const MAX_RECONNECTION: usize = 10;

    fn new(peer_addr: SocketAddr, client_id: PeerId, config: PeerConfig) -> Self {
        Self {
            client_id,
            peer_addr,
            config,
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
            let (tcp, handshake) = self.connect_with_retry(reconnect_delay).await;

            let _ = evt_tx
                .send((
                    self.peer_addr,
                    PeerEvent::Connected {
                        peer_id: handshake.peer_id,
                        peer_extensions: PeerExtensions {
                            fast: handshake.fast_extension,
                            dht: handshake.dht_protocol,
                        },
                    },
                ))
                .await;

            let (reader, mut writer) = tcp.into_split();
            let mut read_task = self.spawn_reader(reader, evt_tx.clone());

            loop {
                tokio::select! {
                    cmd = cmd_rx.recv() => match cmd {
                        None => {
                            read_task.abort();
                            break 'run
                        },
                        Some(msg) => {
                            if writer.write_all(&msg.frame()).await.is_err() {
                                break
                            }
                        },
                    },

                    _ = &mut read_task => break,

                    _ = keepalive.tick() => {
                        if writer.write_all(&Message::KeepAlive.frame()).await.is_err() {
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

    async fn connect_with_retry(&self, mut delay: Duration) -> (TcpStream, Handshake) {
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

    async fn connect(&self) -> Result<(TcpStream, Handshake)> {
        let mut stream = timeout(Self::CONNECT_TIMEOUT, TcpStream::connect(self.peer_addr))
            .await
            .map_err(|_| Error::Timeout)??;

        // Extension configuration during handshake
        let extension_protocol = true; // BEP 10
        let fast_extension = true; // BEP 6
        let dht_protocol = false;
        let info_hash = self.config.info_hash;
        let peer_id = self.client_id;

        let outbound = Handshake::new(
            info_hash,
            peer_id,
            dht_protocol,
            extension_protocol,
            fast_extension,
        );
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

        if inbound.info_hash != self.config.info_hash {
            return Err(Error::InfoHashMismatch);
        }

        if inbound.extension_protocol {
            // Upon connection we share our supported extensions via BEP10
            let mut extensions = HashMap::new();
            extensions.insert("ut_metadata".to_string(), UT_METADATA_EXT_ID); // BEP 9

            let hs = Message::ExtensionHandshake(ExtensionHandshake {
                extensions,
                client: Some("TT".to_string()),
                listen_port: None,
                your_ip: None,
                ipv4: None,
                ipv6: None,
                reqq: None,
                metadata_size: self.config.metadata_size,
            });
            timeout(Self::CONNECT_TIMEOUT, stream.write_all(&hs.frame()))
                .await
                .map_err(|_| Error::Timeout)??;
        }

        Ok((stream, inbound))
    }
}

struct Handshake {
    info_hash: InfoHash,
    peer_id: PeerId,
    dht_protocol: bool,
    extension_protocol: bool,
    fast_extension: bool,
}

impl Handshake {
    // BitTorrent handshake (BEP 3).
    //
    // Offset  Size  Field
    // ------  ----  ------------------------------------------------
    // 0       1     pstrlen      = 19
    // 1       19    pstr         = "BitTorrent protocol"
    // 20      8     reserved     Extension / feature flags
    // 28      20    info_hash    SHA-1 hash of the torrent info dictionary
    // 48      20    peer_id      Peer identifier
    //
    // Total size: 68 bytes.
    //
    // `reserved` bits commonly used:
    //
    // reserved[5] bit 4 (0x10) → BEP 10: Extension Protocol
    // reserved[7] bit 2 (0x04) → BEP 6:  Fast Extension
    // reserved[7] bit 0 (0x01) → BEP 5:  DHT Protocol

    const PSTR: &[u8; 19] = b"BitTorrent protocol";
    const HANDSHAKE_LEN: usize = 68;

    const EXTENSION_PROTOCOL_MASK: u8 = 0b0001_0000;
    const FAST_EXTENSION_MASK: u8 = 0b0000_0100;
    const DHT_PROTOCOL_MASK: u8 = 0b0000_0001;

    fn new(
        info_hash: InfoHash,
        peer_id: PeerId,
        dht_protocol: bool,
        extension_protocol: bool,
        fast_extension: bool,
    ) -> Self {
        Self {
            info_hash,
            peer_id,
            fast_extension,
            extension_protocol,
            dht_protocol,
        }
    }

    fn encode(&self) -> [u8; Self::HANDSHAKE_LEN] {
        let mut out = [0u8; Self::HANDSHAKE_LEN];
        out[0] = Self::PSTR.len() as u8;
        out[1..20].copy_from_slice(Self::PSTR);
        if self.extension_protocol {
            out[25] |= Self::EXTENSION_PROTOCOL_MASK;
        }
        if self.fast_extension {
            out[27] |= Self::FAST_EXTENSION_MASK;
        }
        if self.dht_protocol {
            out[27] |= Self::DHT_PROTOCOL_MASK;
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

        let mut reserved_bytes = [0u8; 8];
        reserved_bytes.copy_from_slice(&buf[20..28]);

        let mut hash_bytes = [0u8; 20];
        hash_bytes.copy_from_slice(&buf[28..48]);

        let mut peer_id_bytes = [0u8; 20];
        peer_id_bytes.copy_from_slice(&buf[48..68]);

        let extension_protocol = (reserved_bytes[5] & Self::EXTENSION_PROTOCOL_MASK) != 0;
        let fast_extension = (reserved_bytes[7] & Self::FAST_EXTENSION_MASK) != 0;
        let dht_protocol = (reserved_bytes[7] & Self::DHT_PROTOCOL_MASK) != 0;

        Ok(Handshake::new(
            InfoHash::from(hash_bytes),
            PeerId::new(peer_id_bytes),
            dht_protocol,
            extension_protocol,
            fast_extension,
        ))
    }
}

// TCP framing for the BitTorrent wire protocol (BEP 3):
//
//   send:    msg.encode() → [id][data...]  →  msg.frame() → [len][id][data...]
//   receive: Message::read_from() strips [len] → [id][data...]  →  Message::decode()
//
//   +------------------+-----+------------------+
//   | length (4 bytes) |  id |  data            |
//   +------------------+-----+------------------+
//
// length = number of bytes after the 4-byte prefix (id + data).
// KeepAlive is the special case: length = 0, no id, no data.

impl Message {
    const MAX_MESSAGE_SIZE: usize = 1024 * 1024; // 1Mb

    fn frame(&self) -> Vec<u8> {
        let payload = self.encode();
        let mut buf = Vec::with_capacity(4 + payload.len());
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&payload);
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

        Ok(Self::decode(&payload)?)
    }
}
