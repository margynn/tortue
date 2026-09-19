use std::{fs, path::PathBuf, thread::sleep, time::Duration};

use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::{ArgAction, Parser, Subcommand};
use crossterm::{
    event::{self, Event, KeyCode},
    terminal::{disable_raw_mode, enable_raw_mode},
};
use indicatif::{ProgressBar, ProgressStyle};
use tortue_lib::{SwarmHandle, download, download_magnet, metainfo};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "tortue")]
#[command(about = "Tortue BitTorrent client in Rust")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Download a torrent
    Download {
        /// Path to .torrent file or magnet URI
        source: String,

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
        Command::Download {
            source,
            out,
            verbose,
        } => {
            init_logging(verbose);

            let dl = if source.starts_with("magnet:") {
                download_magnet(&source, out).await?
            } else {
                let data = fs::read(&source)?;
                download(&data, out).await?
            };

            let bar = ProgressBar::new(0);

            bar.set_style(
                ProgressStyle::default_bar()
                    .template(
                        "{spinner:.green} [{elapsed_precise}] \
                         [{bar:40.cyan/blue}] {pos}/{len} blocks ({percent}%) — {msg}",
                    )
                    .unwrap()
                    .progress_chars("#|."),
            );

            let mut progress = dl.progress;

            tokio::spawn(async move {
                while progress.changed().await.is_ok() {
                    let s = progress.borrow();

                    bar.set_length(s.blocks_total as u64);
                    bar.set_position(s.blocks_done as u64);

                    bar.set_message(format!(
                        "{} seeders, {} leechers, {} in flight \
                         — ↓ {}/s ↑ {}/s {:#?}",
                        s.seeders.len(),
                        s.leechers.len(),
                        s.blocks_in_flight,
                        human_size(s.download_rate as u64),
                        human_size(s.upload_rate as u64),
                        s.status,
                    ));
                }

                bar.finish_with_message("completed");
            });

            let control = dl.handle.clone();
            let mut keyboard = tokio::spawn(async move { keyboard_control(control).await });

            let mut task = dl.task;

            tokio::select! {
                result = &mut task => {
                    keyboard.abort();
                    result??;
                }

                result = &mut keyboard => {
                    result??;
                    task.await??;
                }
            }
        },

        Command::Inspect { path } => {
            let data = fs::read(path)?;
            let m = metainfo(&data).await?;

            println!("{:<14} {}", "Name:", m.name);
            println!("{:<14} {}", "Hash:", hex(m.info_hash.as_ref()));
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

async fn keyboard_control(control: SwarmHandle) -> Result<()> {
    enable_raw_mode()?;

    loop {
        if event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
        {
            match key.code {
                KeyCode::Char('s') => {
                    control.resume().await?;
                },

                KeyCode::Char('p') => {
                    control.pause().await?;
                },

                KeyCode::Char('q') => {
                    control.shutdown().await?;
                    break;
                },

                _ => {},
            }
        }

        sleep(Duration::ZERO);
    }

    disable_raw_mode()?;

    Ok(())
}
