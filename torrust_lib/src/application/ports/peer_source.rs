use std::net::SocketAddr;

use tokio::sync::mpsc;

pub trait PeerSource: Send + 'static {
    async fn run(self, tx: mpsc::Sender<Vec<SocketAddr>>) -> anyhow::Result<()>;
}
