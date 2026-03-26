use anyhow::Result;
use clap::Parser;
use std::fs;
use torrust_lib::{
    metainfo::Mode,
    tracker::{PeerId, TrackerEndpoint, TrackerSession},
};

#[derive(Parser, Debug)]
struct Cli {
    path: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let data = fs::read(&cli.path)?;

    let metainfo = torrust_lib::metainfo::decode(&data)?;

    let endpoint = TrackerEndpoint::parse(&metainfo.announce)?;
    let peer_id = PeerId::generate("TR", "0.1.0");

    let left = match &metainfo.mode {
        Mode::Single { length } => *length as u64,
        Mode::Multiple { files } => files.iter().map(|f| f.length as u64).sum(),
    };

    let mut tracker = TrackerSession::new(endpoint, metainfo.hash, peer_id, 2940, left)?;

    let response = tracker.start().await?;
    println!("{response:#?}");

    Ok(())
}
