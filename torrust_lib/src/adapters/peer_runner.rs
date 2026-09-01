use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::debug;

use crate::domain::peer::{
    AsyncByteReader, Handshake, Input, Message, Output, PeerId, PeerSession,
};
use crate::domain::torrent::Metainfo;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const RECONNECT_DELAY: Duration = Duration::from_secs(2);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("connection timed out")]
    Timeout,

    #[error("info hash mismatch")]
    InfoHashMismatch,

    #[error("protocol error: {0}")]
    Protocol(#[from] crate::domain::peer::Error),

    #[error("peer pool disconnected")]
    PeerPoolGone,
}

impl AsyncByteReader for TcpStream {
    fn read_exact<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = std::io::Result<()>> + 'a {
        async move { AsyncReadExt::read_exact(self, buf).await.map(|_| ()) }
    }
}

type Result<T> = std::result::Result<T, Error>;

pub struct PeerRunner {
    client_id: PeerId,
    peer_addr: SocketAddr,
    metainfo: Arc<Metainfo>,
    cmd_rx: mpsc::Receiver<Input>,
    tx: mpsc::Sender<Output>,
}

impl PeerRunner {
    pub fn new(
        peer_addr: SocketAddr,
        client_id: PeerId,
        metainfo: Arc<Metainfo>,
        cmd_rx: mpsc::Receiver<Input>,
        tx: mpsc::Sender<Output>,
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
        debug!("start_peer");

        let mut session = PeerSession::new();
        let mut reconnect_delay = Duration::ZERO;

        'run: loop {
            let (mut tcp, peer_id) = self.connect_with_retry(reconnect_delay).await;

            // Notify the session (and pool) that we are now connected.
            for out in session.step(Input::Connected {
                peer_id,
                num_pieces: self.metainfo.pieces.len(),
            }) {
                self.tx.send(out).await.map_err(|_| Error::PeerPoolGone)?;
            }

            reconnect_delay = 'session: loop {
                let input = tokio::select! {
                    res = Message::read_from(&mut tcp) => match res {
                        Ok(msg) => Input::MessageReceived(msg),
                        Err(_)  => Input::Disconnected,
                    },
                    // Shutdown when the command channel is dropped.
                    cmd = self.cmd_rx.recv() => match cmd {
                        None => break 'run,
                        Some(cmd) => cmd,
                    },
                };

                for out in session.step(input) {
                    match out {
                        Output::SendToPeer(msg) => {
                            if tcp.write_all(&msg.encode()).await.is_err() {
                                break 'session RECONNECT_DELAY;
                            }
                        },
                        Output::EmitDisconnected => {
                            self.tx.send(out).await.map_err(|_| Error::PeerPoolGone)?;
                            break 'session RECONNECT_DELAY;
                        },
                        _ => self.tx.send(out).await.map_err(|_| Error::PeerPoolGone)?,
                    }
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
                Err(_) => delay = (delay * 2).clamp(RECONNECT_DELAY, MAX_RECONNECT_DELAY),
            }
        }
    }

    async fn connect(&self) -> Result<(TcpStream, PeerId)> {
        let mut stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(self.peer_addr))
            .await
            .map_err(|_| Error::Timeout)??;

        let outbound = Handshake::new(self.metainfo.hash, self.client_id);
        timeout(CONNECT_TIMEOUT, stream.write_all(&outbound.encode()))
            .await
            .map_err(|_| Error::Timeout)??;

        let mut buf = [0u8; Handshake::HANDSHAKE_LEN];
        timeout(CONNECT_TIMEOUT, AsyncReadExt::read_exact(&mut stream, &mut buf))
            .await
            .map_err(|_| Error::Timeout)??;

        let inbound = Handshake::decode(&buf)?;

        if inbound.info_hash != self.metainfo.hash {
            return Err(Error::InfoHashMismatch);
        }

        Ok((stream, inbound.peer_id))
    }
}
