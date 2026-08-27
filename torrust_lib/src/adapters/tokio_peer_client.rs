use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout};

use crate::domain::peer::{
    Handshake, Input, Message, Output, PeerAddr, PeerCommand, PeerEvent,
    PeerId, PeerSession,
};
use crate::domain::torrent::Metainfo;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_MESSAGE_SIZE: u32 = 1 << 20;

pub struct TokioPeerClient {
    client_id: PeerId,
    peer_addr: PeerAddr,
    metainfo: Metainfo,
}

impl TokioPeerClient {
    pub fn new(
        peer_addr: PeerAddr,
        client_id: PeerId,
        metainfo: Metainfo,
    ) -> Self {
        Self { client_id, peer_addr, metainfo }
    }

    pub async fn run(
        self,
        mut cmd_rx: mpsc::Receiver<PeerCommand>,
        event_tx: mpsc::Sender<PeerEvent>,
    ) {
        let (mut session, initial) = PeerSession::new(self.peer_addr);
        let mut conn: Option<TcpStream> = None;
        let mut retry_at = Instant::now();

        for out in initial {
            if let Output::ScheduleRetry(d) = out {
                retry_at = Instant::now() + d;
            }
        }

        loop {
            let (input, drop_conn) = if conn.is_some() {
                tokio::select! {
                    cmd = cmd_rx.recv() => {
                        (Input::CommandReceived(cmd.unwrap_or(PeerCommand::Shutdown)), false)
                    }
                    res = read_message(conn.as_mut().unwrap()) => match res {
                        Ok(msg) => (Input::MessageReceived(msg), false),
                        Err(_) => (Input::Disconnected, true),
                    }
                }
            } else {
                tokio::select! {
                    cmd = cmd_rx.recv() => {
                        (Input::CommandReceived(cmd.unwrap_or(PeerCommand::Shutdown)), false)
                    }
                    _ = tokio::time::sleep_until(retry_at) => {
                        match self.connect().await {
                            Ok((stream, peer_id)) => {
                                conn = Some(stream);
                                (Input::Connected { peer_id }, false)
                            }
                            Err(_) => (Input::ConnectionFailed, false),
                        }
                    }
                }
            };

            if drop_conn {
                conn = None;
            }

            let mut should_stop = false;
            for out in session.step(input) {
                match out {
                    Output::Send(msg) => {
                        if let Some(ref mut stream) = conn {
                            let _ = send(stream, msg).await;
                        }
                    },
                    Output::EmitEvent(evt) => {
                        let _ = event_tx.send(evt).await;
                    },
                    Output::ScheduleRetry(d) => {
                        retry_at = Instant::now() + d;
                    },
                    Output::Stop => should_stop = true,
                }
            }

            if should_stop {
                break;
            }
        }
    }

    async fn connect(&self) -> Result<(TcpStream, PeerId), ()> {
        let addr =
            std::net::SocketAddr::new(self.peer_addr.0, self.peer_addr.1);

        let mut stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| ())?
            .map_err(|_| ())?;

        let outbound = Handshake::new(self.metainfo.hash, self.client_id);
        timeout(CONNECT_TIMEOUT, stream.write_all(&outbound.encode()))
            .await
            .map_err(|_| ())?
            .map_err(|_| ())?;

        let mut buf = [0u8; Handshake::HANDSHAKE_LEN];
        timeout(CONNECT_TIMEOUT, stream.read_exact(&mut buf))
            .await
            .map_err(|_| ())?
            .map_err(|_| ())?;

        let inbound = Handshake::decode(&buf).map_err(|_| ())?;

        if inbound.info_hash != self.metainfo.hash {
            return Err(());
        }

        Ok((stream, inbound.peer_id))
    }
}

async fn read_message(stream: &mut TcpStream) -> Result<Message, ()> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.map_err(|_| ())?;
    let length = u32::from_be_bytes(len_buf);

    if length == 0 {
        return Ok(Message::KeepAlive);
    }
    if length > MAX_MESSAGE_SIZE {
        return Err(());
    }

    let mut payload = vec![0u8; length as usize];
    stream.read_exact(&mut payload).await.map_err(|_| ())?;

    Message::decode(&payload).map_err(|_| ())
}

async fn send(stream: &mut TcpStream, msg: Message) -> Result<(), ()> {
    let buf = msg.encode().map_err(|_| ())?;
    stream.write_all(&buf).await.map_err(|_| ())?;
    Ok(())
}
