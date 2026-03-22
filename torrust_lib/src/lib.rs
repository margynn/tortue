#![allow(dead_code)] // todo: remove
mod bencode;
pub mod byte_parser;

pub fn run() -> Result<String, anyhow::Error> {
    Ok("hello torrust".to_string())
}
