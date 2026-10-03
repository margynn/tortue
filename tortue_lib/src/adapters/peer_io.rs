use rand::RngExt;
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    collections::HashMap,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
    time::timeout,
};

use crate::{
    adapters::swarm_registry::{InboundPeer, SwarmRegistry},
    application::ports::peer_connector::PeerConnector,
    domain::{
        message::{
            Error as DecodeError, ExtensionHandshake, Message, UT_METADATA_EXT_ID, UT_PEX_EXT_ID,
        },
        peer::{self, Handshake, PeerEvent, PeerExtensions, PeerId},
        torrent::InfoHash,
    },
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("peer error: {0}")]
    Peer(#[from] peer::Error),

    #[error("connection timed out")]
    Timeout,

    #[error("info hash mismatch")]
    InfoHashMismatch,

    #[error("message too large")]
    MessageTooLarge,

    #[error("message decode: {0}")]
    MessageDecode(#[from] DecodeError),

    #[error("max peer connection attempts")]
    MaxAttemptsExceeded,

    #[error("peer connection cancelled")]
    Cancelled,

    #[error("connection to myself")]
    SelfConnection,

    #[error("inbound swarm channel closed")]
    SwarmChannelClosed,
}

type Result<T> = std::result::Result<T, Error>;

pub struct TcpPeerConnector {
    client_id: PeerId,
    peer_config: PeerConfig,
    peer_cancels: HashMap<SocketAddr, watch::Sender<bool>>,
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
            peer_cancels: HashMap::new(),
        }
    }
}

impl PeerConnector for TcpPeerConnector {
    type Inbound = InboundPeer;

    fn connect(
        &mut self,
        addr: SocketAddr,
        cmd_rx: mpsc::Receiver<Message>,
        evt_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    ) {
        let (cancel_tx, cancel_rx) = watch::channel(false);
        self.peer_cancels.insert(addr, cancel_tx);
        let mut runner = TcpPeerIO::new(addr, self.client_id, self.peer_config, cancel_rx);
        tokio::spawn(async move { runner.run(cmd_rx, evt_tx).await });
    }

    fn accept(
        &mut self,
        inbound: Self::Inbound,
        cmd_rx: mpsc::Receiver<Message>,
        evt_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    ) {
        let (cancel_tx, cancel_rx) = watch::channel(false);
        self.peer_cancels.insert(inbound.addr, cancel_tx);
        let info_hash = self.peer_config.info_hash;
        tokio::spawn(async move {
            let addr = inbound.addr;

            if inbound.handshake.info_hash != info_hash {
                // Programming error the connector should never be sent mismatching inbound
                drop(inbound);
            } else {
                let mut cmd_rx = cmd_rx;
                let mut cancel_rx = cancel_rx;
                run_session(
                    inbound.stream,
                    &inbound.handshake,
                    addr,
                    &mut cmd_rx,
                    &evt_tx,
                    &mut cancel_rx,
                )
                .await;
            }

            let _ = evt_tx.send((addr, PeerEvent::Disconnected)).await;
        });
    }

    fn disconnect(&mut self, addr: SocketAddr) {
        if let Some(tx) = self.peer_cancels.remove(&addr) {
            let _ = tx.send(true);
        }
    }
}

enum SessionExit {
    Stop,
    Reconnect,
}

async fn run_session(
    stream: TcpStream,
    handshake: &Handshake,
    peer_addr: SocketAddr,
    cmd_rx: &mut mpsc::Receiver<Message>,
    evt_tx: &mpsc::Sender<(SocketAddr, PeerEvent)>,
    cancel_rx: &mut watch::Receiver<bool>,
) -> SessionExit {
    const PEER_IDLE_TIMEOUT: Duration = Duration::from_secs(180);
    const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(120);
    let mut keepalive = tokio::time::interval_at(
        tokio::time::Instant::now() + KEEPALIVE_INTERVAL,
        KEEPALIVE_INTERVAL,
    );

    let _ = evt_tx
        .send((
            peer_addr,
            PeerEvent::Connected {
                peer_id: handshake.peer_id,
                peer_extensions: PeerExtensions {
                    fast: handshake.fast_extension,
                    dht: handshake.dht_protocol,
                },
            },
        ))
        .await;

    let reader_tx = evt_tx.clone();
    let (mut reader, mut writer) = stream.into_split();
    let mut read_task = tokio::spawn(async move {
        loop {
            let msg = match timeout(PEER_IDLE_TIMEOUT, Message::read_from(&mut reader)).await {
                Ok(Ok(msg)) => msg,
                _ => return,
            };
            if reader_tx
                .send((peer_addr, PeerEvent::MessageReceived(msg)))
                .await
                .is_err()
            {
                return;
            }
        }
    });

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => match cmd {
                None => {
                    // Channel closed -> disconnect the peer
                    read_task.abort();
                    return SessionExit::Stop;
                },
                Some(msg) => {
                    if writer.write_all(&msg.frame()).await.is_err() {
                        break
                    }
                },
            },

            _ = cancel_rx.changed() => {
                read_task.abort();
                return SessionExit::Stop;
            }

            _ = &mut read_task => break,

            _ = keepalive.tick() => {
                if writer.write_all(&Message::KeepAlive.frame()).await.is_err() {
                   break
                }
             },
        }
    }

    SessionExit::Reconnect
}

const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

struct TcpPeerIO {
    client_id: PeerId,
    peer_addr: SocketAddr,
    config: PeerConfig,
    cancel_rx: watch::Receiver<bool>,
}

impl TcpPeerIO {
    const RECONNECT_DELAY: Duration = Duration::from_secs(4);
    const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(90);
    const MAX_ATTEMPTS: usize = 5;

    fn new(
        peer_addr: SocketAddr,
        client_id: PeerId,
        config: PeerConfig,
        cancel_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            client_id,
            peer_addr,
            config,
            cancel_rx,
        }
    }

    async fn run(
        &mut self,
        mut cmd_rx: mpsc::Receiver<Message>,
        evt_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    ) -> Result<()> {
        let mut reconnect_delay = Duration::ZERO;

        'run: loop {
            let (stream, handshake) = self.connect_with_retry(reconnect_delay).await?;

            match run_session(
                stream,
                &handshake,
                self.peer_addr,
                &mut cmd_rx,
                &evt_tx,
                &mut self.cancel_rx,
            )
            .await
            {
                SessionExit::Stop => break 'run,
                SessionExit::Reconnect => {
                    reconnect_delay = Self::RECONNECT_DELAY;
                },
            }
        }

        // Sentinel: ensures Coordinator always receives Disconnected even on clean exit.
        let _ = evt_tx.send((self.peer_addr, PeerEvent::Disconnected)).await;
        Ok(())
    }

    async fn connect_with_retry(&mut self, mut delay: Duration) -> Result<(TcpStream, Handshake)> {
        let mut attempts = 0;
        loop {
            if attempts >= Self::MAX_ATTEMPTS {
                return Err(Error::MaxAttemptsExceeded);
            }
            attempts += 1;

            let jitter = rand::rng().random_range(0.8..=1.2);
            let sleep_for = delay.mul_f64(jitter);

            tokio::select! {
                _ = tokio::time::sleep(sleep_for) => {},
                _ = self.cancel_rx.changed() => return Err(Error::Cancelled),
            };

            match self.connect().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    tracing::debug!(
                        addr = %self.peer_addr,
                        error = %e,
                        "peer connection failed, retrying"
                    );

                    delay = (delay * 2).clamp(Self::RECONNECT_DELAY, Self::MAX_RECONNECT_DELAY);
                },
            }
        }
    }

    async fn connect(&self) -> Result<(TcpStream, Handshake)> {
        let mut stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(self.peer_addr))
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
        timeout(CONNECT_TIMEOUT, stream.write_all(&outbound.encode()))
            .await
            .map_err(|_| Error::Timeout)??;

        let mut buf = [0u8; Handshake::HANDSHAKE_LEN];
        timeout(
            CONNECT_TIMEOUT,
            AsyncReadExt::read_exact(&mut stream, &mut buf),
        )
        .await
        .map_err(|_| Error::Timeout)??;

        let inbound = Handshake::decode(&buf)?;

        if inbound.peer_id == peer_id {
            return Err(Error::SelfConnection);
        }
        if inbound.info_hash != self.config.info_hash {
            return Err(Error::InfoHashMismatch);
        }

        if inbound.extension_protocol {
            // Upon connection we share our supported extensions via BEP10
            let mut extensions = HashMap::new();
            extensions.insert("ut_metadata".to_string(), UT_METADATA_EXT_ID); // BEP 9
            extensions.insert("ut_pex".to_string(), UT_PEX_EXT_ID); // BEP 11

            let hs = Message::ExtensionHandshake(ExtensionHandshake {
                extensions,
                metadata_size: self.config.metadata_size,
            });
            timeout(CONNECT_TIMEOUT, stream.write_all(&hs.frame()))
                .await
                .map_err(|_| Error::Timeout)??;
        }

        Ok((stream, inbound))
    }
}

impl Message {
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

struct TcpPeerListenner {
    client_id: PeerId,
    port: u16,
    swarm_registry: SwarmRegistry,
}

impl TcpPeerListenner {
    pub fn new(client_id: PeerId, port: u16, swarm_registry: SwarmRegistry) -> Self {
        Self {
            client_id,
            port,
            swarm_registry,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let ipv4 = Self::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, self.port)))?;
        let ipv6 = Self::bind(SocketAddr::from((Ipv6Addr::UNSPECIFIED, self.port)))?;
        tokio::try_join!(self.accept_loop(ipv4), self.accept_loop(ipv6),)?;
        Ok(())
    }

    fn bind(addr: SocketAddr) -> Result<TcpListener> {
        let domain = if addr.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };
        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
        if addr.is_ipv6() {
            socket.set_only_v6(true)?;
        }
        socket.bind(&addr.into())?;
        socket.listen(1024)?;
        socket.set_nonblocking(true)?;
        TcpListener::from_std(socket.into()).map_err(|err| Error::Io(err))
    }

    async fn accept_loop(&self, listener: TcpListener) -> Result<()> {
        loop {
            let (stream, _) = listener.accept().await?;
            let registry = self.swarm_registry.clone();
            let client_id = self.client_id;

            tokio::spawn(async move {
                let _ = Self::handle_peer(stream, client_id, registry).await;
            });
        }
    }

    async fn handle_peer(
        mut stream: TcpStream,
        client_id: PeerId,
        registry: SwarmRegistry,
    ) -> Result<()> {
        let addr = stream.peer_addr()?;

        let mut buf = [0u8; Handshake::HANDSHAKE_LEN];
        timeout(
            CONNECT_TIMEOUT,
            AsyncReadExt::read_exact(&mut stream, &mut buf),
        )
        .await
        .map_err(|_| Error::Timeout)??;

        let inbound = Handshake::decode(&buf)?;

        if inbound.peer_id == client_id {
            return Err(Error::SelfConnection);
        }

        let Some(tx) = registry.route(inbound.info_hash) else {
            // unknown torrent, closing the stream
            drop(stream);
            return Ok(());
        };

        let outbound = Handshake::new(
            inbound.info_hash,
            client_id,
            false, // DHT
            true,  // BEP 10
            true,  // Fast extension
        );

        timeout(CONNECT_TIMEOUT, stream.write_all(&outbound.encode()))
            .await
            .map_err(|_| Error::Timeout)??;

        if inbound.extension_protocol {
            let extensions = HashMap::from([
                ("ut_metadata".to_owned(), UT_METADATA_EXT_ID),
                ("ut_pex".to_owned(), UT_PEX_EXT_ID),
            ]);
            let message = Message::ExtensionHandshake(ExtensionHandshake {
                extensions,
                metadata_size: None,
            });
            timeout(CONNECT_TIMEOUT, stream.write_all(&message.frame()))
                .await
                .map_err(|_| Error::Timeout)??;
        }

        let peer = InboundPeer {
            addr,
            handshake: inbound,
            stream,
        };
        timeout(CONNECT_TIMEOUT, tx.send(peer))
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::SwarmChannelClosed)?;
        Ok(())
    }
}
