use std::path::PathBuf;
use std::time::Instant;

use sha1::{Digest, Sha1};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use crate::metainfo::{Metainfo, Mode};
use crate::peer::PeerId;
use crate::peer::bitfield::Bitfield;

const BLOCK_SIZE: u32 = 16 * 1024; // 16kb

#[derive(Clone, Copy, Debug)]
enum BlockState {
    Missing,
    Reserved { peer: PeerId, at: Instant },
    Received,
}

#[derive(Debug)]
struct Piece {
    blocks: Vec<BlockState>,
    buffer: Vec<u8>,
    received: usize,
}

#[derive(Debug)]
pub enum WriteResult {
    BlockStored,
    PieceCompleted(u32),
    PieceInvalid(u32),
}

pub struct PieceManager {
    metainfo: Metainfo,
    bitfield: Bitfield,
    pieces: Vec<Piece>,
    files: Vec<OutputFile>, // instead of single file
}

struct OutputFile {
    file: File,
    length: u64,
    offset: u64, // global start offset in torrent
}

impl PieceManager {
    pub async fn new(
        metainfo: Metainfo,
        root: PathBuf,
    ) -> std::io::Result<Self> {
        let mut files = Vec::new();
        let mut offset = 0u64;

        match &metainfo.mode {
            Mode::Single { length } => {
                let path = root.join(&metainfo.name);
                if let Some(parent) = path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                let file = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .open(&path)
                    .await?;
                file.set_len(*length).await?;
                files.push(OutputFile { file, length: *length, offset });
            },

            Mode::Multiple { files: meta_files } => {
                let base = root.join(&metainfo.name);
                for f in meta_files {
                    let path = base.join(PathBuf::from_iter(&f.path));
                    if let Some(parent) = path.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    let file = OpenOptions::new()
                        .create(true)
                        .read(true)
                        .write(true)
                        .open(&path)
                        .await?;
                    file.set_len(f.length).await?;
                    files.push(OutputFile { file, length: f.length, offset });
                    offset += f.length;
                }
            },
        }

        // pieces init unchanged
        let mut pieces = Vec::with_capacity(metainfo.pieces.len());

        for (i, _) in metainfo.pieces.iter().enumerate() {
            let piece_len = Self::piece_length(&metainfo, i);
            let blocks = (piece_len as u32 + BLOCK_SIZE - 1) / BLOCK_SIZE;

            pieces.push(Piece {
                blocks: vec![BlockState::Missing; blocks as usize],
                buffer: vec![0; piece_len as usize],
                received: 0,
            });
        }

        Ok(Self {
            bitfield: Bitfield::new(metainfo.pieces.len()),
            metainfo,
            pieces,
            files,
        })
    }

    fn piece_length(meta: &Metainfo, index: usize) -> u64 {
        let last = meta.pieces.len() - 1;
        if index == last {
            meta.size() - (meta.piece_length * index as u64)
        } else {
            meta.piece_length
        }
    }

    fn block_index(offset: u32) -> usize {
        (offset / BLOCK_SIZE) as usize
    }

    pub fn has_piece(&self, piece: u32) -> bool {
        self.bitfield.has_bit(piece as usize).unwrap()
    }

    pub fn missing_blocks(&self, piece: u32) -> impl Iterator<Item = u32> + '_ {
        self.pieces[piece as usize]
            .blocks
            .iter()
            .enumerate()
            .filter(|(_, b)| matches!(b, BlockState::Missing))
            .map(|(i, _)| i as u32)
    }

    // pub async fn write_block(
    //     &mut self,
    //     piece: u32,
    //     offset: u32,
    //     data: &[u8],
    // ) -> std::io::Result<WriteResult> {
    //     return Ok();
    // let p = &mut self.pieces[piece as usize];
    // let idx = Self::block_index(offset);

    // // Ignore duplicates safely
    // if matches!(p.blocks[idx], BlockState::Received) {
    //     return Ok(WriteResult::BlockStored);
    // }

    // // Copy into piece buffer
    // let start = offset as usize;
    // let end = start + data.len();
    // p.buffer[start..end].copy_from_slice(data);

    // p.blocks[idx] = BlockState::Received;
    // p.received += 1;

    // if p.received != p.blocks.len() {
    //     return Ok(WriteResult::BlockStored);
    // }

    // // Piece complete → verify hash
    // let expected = self.metainfo.pieces[piece as usize];
    // let digest = Sha1::digest(p.buffer.clone());
    // let mut actual = [0u8; 20];
    // actual.copy_from_slice(&digest);

    // if actual != expected {
    //     // reset
    //     for b in &mut p.blocks {
    //         *b = BlockState::Missing;
    //     }
    //     p.received = 0;
    //     return Ok(WriteResult::PieceInvalid(piece));
    // }

    // // Write to disk
    // let global_offset = piece as u64 * self.metainfo.piece_length;

    // self.file.seek(std::io::SeekFrom::Start(global_offset)).await?;
    // self.file.write_all(&p.buffer).await?;

    // let _ = self.bitfield.set_bit(piece as usize);

    // Ok(WriteResult::PieceCompleted(piece))
    // }
}
