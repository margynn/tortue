use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::{ArgAction, Parser, Subcommand};
use torrust_lib::{download, metainfo};
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
            let data = fs::read(path)?;
            let m = metainfo(&data).await?;

            println!("{:<14} {}", "Name:", m.name);
            println!("{:<14} {}", "Hash:", hex(m.hash.as_ref()));
            println!("{:<14} {}", "Size:", human_size(m.total_size()));
            println!(
                "{:<14} {} × {}",
                "Pieces:",
                m.pieces.len(),
                human_size(m.piece_length as u64)
            );
            println!("{:<14} {}", "Trackers:", m.announce.len());
            for url in &m.announce {
                println!("  - {url}");
            }
            if let Some(c) = &m.comment {
                println!("{:<14} {c}", "Comment:");
            }
            if let Some(c) = &m.created_by {
                println!("{:<14} {c}", "Created by:");
            }
            if let Some(t) = m.created_at {
                println!("{:<14} {}", "Created at:", human_timestamp(t));
            }
        },
    }

    Ok(())
}

fn human_timestamp(ts: i64) -> String {
    DateTime::from_timestamp(ts, 0)
        .map(|dt: DateTime<Utc>| dt.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_else(|| ts.to_string())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn human_size(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    match bytes {
        b if b >= GIB => format!("{:.2} GiB", b as f64 / GIB as f64),
        b if b >= MIB => format!("{:.2} MiB", b as f64 / MIB as f64),
        b if b >= KIB => format!("{:.2} KiB", b as f64 / KIB as f64),
        b => format!("{b} B"),
    }
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
