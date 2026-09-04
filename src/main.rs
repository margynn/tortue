use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use clap::{ArgAction, Parser, Subcommand};
use torrust_lib::download;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "torrust")]
#[command(about = "A BitTorrent client")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Download a torrent
    Download {
        /// Path to the .torrent file
        path: PathBuf,

        /// Output directory
        #[arg(short, long, default_value = "./")]
        out: PathBuf,

        /// Increase log verbosity (-v, -vv, -vvv)
        #[arg(short, long, action = ArgAction::Count)]
        verbose: u8,
    },

    /// Inspect a .torrent file
    Inspect {
        /// Path to the .torrent file
        path: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Download { path, out, verbose } => {
            init_logging(verbose);

            let data = fs::read(path)?;
            download(&data, out).await?;
        },

        Command::Inspect { path } => {
            let _data = fs::read(path)?;
            // let metainfo = Metainfo::try_from(data.as_ref())?;

            // println!("{metainfo:#?}");
        },
    }

    Ok(())
}

fn init_logging(verbose: u8) {
    let level = match verbose {
        0 => "off",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };

    tracing_subscriber::fmt()
        .with_target(false)
        .with_env_filter(EnvFilter::new(level))
        .init();
}
