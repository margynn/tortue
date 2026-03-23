use anyhow::Result;
use clap::Parser;
use std::fs;

#[derive(Parser)]
struct Cli {
    path: String,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let data = fs::read(&cli.path)?;
    let decoded = torrust_lib::bencode::decode(&data)?;
    println!("{:#?}", decoded);
    Ok(())
}
