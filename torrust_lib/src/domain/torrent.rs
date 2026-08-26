mod errors;
mod parse;
mod torrent;

pub use errors::{Error, Result};
pub use torrent::{
    File, InfoHash, Metainfo, Mode, PIECE_HASH_LEN, PieceHash, decode,
};
