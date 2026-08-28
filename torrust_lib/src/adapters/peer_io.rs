use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout};

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
}

impl AsyncByteReader for TcpStream {
    fn read_exact<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = std::io::Result<()>> + Send + 'a {
        async move { AsyncReadExt::read_exact(self, buf).await.map(|_| ()) }
    }
}
type Result<T> = std::result::Result<T, Error>;

struct PeerIO {
    client_id: PeerId,
    peer_addr: SocketAddr,
    metainfo: Metainfo,
    conn: Option<TcpStream>,
    retry_at: Instant,
}

impl PeerIO {
    pub fn new(
        peer_addr: SocketAddr,
        client_id: PeerId,
        metainfo: Metainfo,
    ) -> Self {
        Self {
            client_id,
            peer_addr,
            metainfo,
            conn: None,
            retry_at: Instant::now(),
        }
    }

    pub async fn run(
        &mut self,
        mut cmd_rx: mpsc::Receiver<Input>,
        event_tx: mpsc::Sender<Output>,
    ) {
        let mut session = PeerSession::new(self.peer_addr);

        loop {
            let input = self.next_input(&mut cmd_rx).await;
            let outputs = session.step(input);
            if self.handle_outputs(outputs, &event_tx).await {
                break;
            }
        }
    }

    async fn next_input(
        &mut self,
        cmd_rx: &mut mpsc::Receiver<Input>,
    ) -> Input {
        tokio::select! {
            cmd = cmd_rx.recv() => cmd.unwrap_or(Input::Shutdown),

            res = Message::read_from(self.conn.as_mut().unwrap()), if self.conn.is_some() => match res {
                Ok(msg) => Input::MessageReceived(msg),
                Err(_)  => { self.conn = None; Input::Disconnected }
            },

            _ = tokio::time::sleep_until(self.retry_at), if self.conn.is_none() => {
                match self.connect().await {
                    Ok((stream, peer_id)) => {
                        self.conn = Some(stream);
                        Input::Connected { peer_id, num_pieces: self.metainfo.pieces.len() }
                    }
                    Err(_) => Input::ConnectionFailed,
                }
            }
        }
    }

    async fn handle_outputs(
        &mut self,
        outputs: Vec<Output>,
        event_tx: &mpsc::Sender<Output>,
    ) -> bool {
        let mut should_stop = false;
        for out in outputs {
            match out {
                Output::SendToPeer(msg) => {
                    if let Some(ref mut stream) = self.conn {
                        if let Ok(buf) = msg.encode() {
                            let _ = stream.write_all(&buf).await;
                        };
                    }
                },
                Output::ScheduleRetry(d) => self.retry_at = Instant::now() + d,
                Output::Stop => should_stop = true,
                Output::EmitConnected => {
                    let _ = event_tx.send(Output::EmitConnected).await;
                },
                Output::EmitDisconnected => {
                    let _ = event_tx.send(Output::EmitDisconnected).await;
                },
                Output::EmitMessage(msg) => {
                    let _ = event_tx.send(Output::EmitMessage(msg)).await;
                },
            }
        }
        should_stop
    }

    async fn connect(&self) -> Result<(TcpStream, PeerId)> {
        let mut stream =
            timeout(CONNECT_TIMEOUT, TcpStream::connect(self.peer_addr))
                .await
                .map_err(|_| Error::Timeout)??;

        let outbound = Handshake::new(self.metainfo.hash, self.client_id);
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

        if inbound.info_hash != self.metainfo.hash {
            return Err(Error::InfoHashMismatch);
        }

        Ok((stream, inbound.peer_id))
    }
}
