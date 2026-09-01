use std::fs;

use anyhow::Result;
use clap::Parser;
use torrust_lib::download;

#[derive(Parser, Debug)]
struct Cli {
    path: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    let data = fs::read(&cli.path)?;
    download(data.as_ref()).await?;
    Ok(())
}
