use anyhow::Result;
use clap::Parser;
use std::fs;

#[derive(Parser)]
struct Cli {
    path: String,
}

#[tokio::main] // <- makes main async
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let data = fs::read(&cli.path)?;

    let metainfo = torrust_lib::metainfo::decode(&data)?;
    let peer_id = torrust_lib::tracker::PeerId::generate("test");
    let tracker = torrust_lib::tracker::Tracker::new(metainfo.announce.clone(), peer_id, 2940)?;
    let response = tracker.announce(&metainfo).await?;
    println!("{response:#?}");
    Ok(())
}
