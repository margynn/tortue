use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::domain::peer::{
    AsyncByteReader, Handshake, Input, Message, Output, PeerId, PeerSession,
};
use crate::domain::torrent::Metainfo;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, thiserror::Error)]
enum Error {
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

fn extract_retry(outputs: Vec<Output>) -> Duration {
    outputs
        .into_iter()
        .find_map(|o| match o {
            Output::ScheduleRetry(d) => Some(d),
            _ => None,
        })
        .unwrap_or(Duration::from_secs(2))
}

pub struct PeerRunner<'a> {
    client_id: PeerId,
    peer_addr: SocketAddr,
    metainfo: &'a Metainfo,
}

impl<'a> PeerRunner<'a> {
    pub fn new(peer_addr: SocketAddr, client_id: PeerId, metainfo: &'a Metainfo) -> Self {
        Self { client_id, peer_addr, metainfo }
    }

    pub async fn run(&mut self, mut cmd_rx: mpsc::Receiver<Input>, tx: mpsc::Sender<Output>) -> Result<()> {
        let mut session = PeerSession::new(self.peer_addr);
        let mut delay = Duration::ZERO;

        'run: loop {
            let (mut tcp, _peer_id) = self.connect_with_retry(&mut session, delay).await;

            delay = 'session: loop {
                let input = next_input(&mut tcp, &mut cmd_rx).await;

                for out in session.step(input) {
                    match out {
                        Output::SendToPeer(msg) => {
                            if tcp.write_all(&msg.encode()).await.is_err() {
                                break 'session Duration::ZERO;
                            }
                        }
                        Output::ScheduleRetry(d) => break 'session d,
                        Output::Stop             => break 'run,
                        _                        => tx.send(out).await.map_err(|_| Error::PeerPoolGone)?,
                    }
                }
            };
        }

        Ok(())
    }

    async fn connect_with_retry(&self, session: &mut PeerSession, initial_delay: Duration) -> (TcpStream, PeerId) {
        let mut delay = initial_delay;
        loop {
            tokio::time::sleep(delay).await;
            match self.connect().await {
                Ok(result) => return result,
                Err(_)     => delay = extract_retry(session.step(Input::ConnectionFailed)),
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

async fn next_input(tcp: &mut TcpStream, cmd_rx: &mut mpsc::Receiver<Input>) -> Input {
    tokio::select! {
        res = Message::read_from(tcp) => match res {
            Ok(msg) => Input::MessageReceived(msg),
            Err(_)  => Input::Disconnected,
        },
        cmd = cmd_rx.recv() => cmd.unwrap_or(Input::Shutdown),
    }
}
