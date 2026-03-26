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
    let peer_id = PeerId::generate("TR", "0.1.0");

    println!("{:#?}", metainfo.announce);
    println!("{:#?}", metainfo.announce_list);

    let left = match &metainfo.mode {
        Mode::Single { length } => *length as u64,
        Mode::Multiple { files } => files.iter().map(|f| f.length as u64).sum(),
    };

    for tier in metainfo.announce_list {
        for tracker in tier {
            let endpoint = match TrackerEndpoint::parse(&tracker) {
                Ok(e) => e,
                _ => continue,
            };
            let mut session =
                match TrackerSession::new(endpoint, metainfo.hash, peer_id, 4444, left) {
                    Ok(t) => t,
                    _ => continue,
                };
            let response = match session.start().await {
                Ok(r) => r,
                _ => continue,
            };
            println!("{response:#?}");
            break;
        }
    }

    Ok(())
}
