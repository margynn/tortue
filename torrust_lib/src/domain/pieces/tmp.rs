// use std::path::PathBuf;
// use std::time::{Duration, Instant};

// use sha1::{Digest, Sha1};
// use tokio::fs::{File, OpenOptions};
// use tokio::io::{AsyncSeekExt, AsyncWriteExt};

// use super::errors::Result;
// use super::piece::{BLOCK_SIZE, BlockState, Piece};
// use crate::peer::bitfield::Bitfield;
// use crate::torrent::{Metainfo, Mode};

// // APPLICATION CODE LATTER

// pub async fn handle_block(
//     piece_manager: &mut PieceManager,
//     storage: &mut TokioStorage,
//     piece: u32,
//     offset: usize,
//     data: Vec<u8>,
// ) -> Result<()> {
//     let event = piece_manager.receive_block(piece, offset, data)?;

//     match event {
//         PieceEvent::BlockReceived => {},

//         PieceEvent::PieceInvalid { piece } => {
//             println!("piece {piece} invalid");
//         },

//         PieceEvent::PieceCompleted { piece, command } => {
//             storage.execute(command).await?;

//             piece_manager.storage_completed(piece);
//         },
//     }

//     Ok(())
// }
