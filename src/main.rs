use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use torrust_lib::download;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
struct Cli {
    path: String,

    #[arg(short, long, default_value = "./")]
    out: PathBuf,

    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_target(false)
        .with_env_filter(EnvFilter::new(&cli.log_level))
        .init();

    let data = fs::read(&cli.path)?;
    download(data.as_ref(), cli.out).await?;

    Ok(())
}
