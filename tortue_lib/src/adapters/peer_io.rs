use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

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

        let extension_protocol = true; // BEP 10
        let dht_protocol = false;
        let fast_extension = false;
        let info_hash = self.metainfo.hash;
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

        if inbound.info_hash != self.metainfo.hash {
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
                metadata_size: Some(self.metainfo.info_bytes.len()),
            });
            timeout(Self::CONNECT_TIMEOUT, stream.write_all(&hs.encode()))
                .await
                .map_err(|_| Error::Timeout)??;
        }

        Ok((stream, inbound))
    }
}

pub struct Handshake {
    pub info_hash: InfoHash,
    pub peer_id: PeerId,
    pub dht_protocol: bool,
    pub extension_protocol: bool,
    pub fast_extension: bool,
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

impl Message {
    const MAX_MESSAGE_SIZE: usize = 1024 * 1024; // 1Mb

    async fn read_from<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self> {
        // BitTorrent message framing (BEP 3):
        //
        // Every message is prefixed with a 4-byte big-endian length:
        //
        //   +-------------------+----------------------+
        //   | length (4 bytes)  | payload (length bytes)|
        //   +-------------------+----------------------+
        //
        // `length` does not include the 4-byte length prefix itself.
        //
        // A length of 0 is a keep-alive message:
        //
        //   +-------------------+
        //   | 0x00 00 00 00     |
        //   +-------------------+
        //
        // For regular messages, the first byte of the payload is the
        // BitTorrent message ID:
        //
        //   +-------------------+------+----------------+
        //   | length (4 bytes)  |  ID  | payload        |
        //   +-------------------+------+----------------+

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
