use std::path::PathBuf;
use std::time::{Duration, Instant};

use sha1::{Digest, Sha1};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use super::errors::Result;
use super::piece::{BLOCK_SIZE, BlockState, Piece};
use crate::peer::bitfield::Bitfield;
use crate::torrent::{Metainfo, Mode};

#[derive(Debug)]
#[allow(dead_code)]
pub enum WriteResult {
    BlockStored,
    PieceCompleted(u32),
    PieceInvalid(u32),
}

pub struct PieceManager {
    metainfo: Metainfo,
    bitfield: Bitfield,
    pieces: Vec<Piece>,
    files: Vec<OutputFile>,
}

struct OutputFile {
    file: File,
    length: u64,
    offset: u64, // global start offset in torrent
}

pub struct BlockRange {
    pub begin: usize,
    pub length: usize,
}

impl PieceManager {
    pub async fn new(metainfo: Metainfo, root: PathBuf) -> Result<Self> {
        let files = Self::create_files(&metainfo, root).await?;
        let mut pieces = Vec::with_capacity(metainfo.pieces.len());

        for (i, _) in metainfo.pieces.iter().enumerate() {
            // last piece is cropped
            let is_last = i == metainfo.pieces.len() - 1;
            let piece_length = if is_last {
                metainfo.size() - (metainfo.piece_length * i as u64)
            } else {
                metainfo.piece_length
            };
            let blocks = (piece_length as usize).div_ceil(BLOCK_SIZE);
            pieces.push(Piece {
                blocks: vec![BlockState::Missing; blocks as usize],
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

    async fn create_files(
        metainfo: &Metainfo,
        root: PathBuf,
    ) -> Result<Vec<OutputFile>> {
        let mut files = Vec::new();
        let mut offset = 0u64;

        let base = root.join(&metainfo.name);

        match &metainfo.mode {
            Mode::Single { length } => {
                if let Some(parent) = base.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                let file = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .read(true)
                    .write(true)
                    .open(&base)
                    .await?;
                file.set_len(*length).await?;
                files.push(OutputFile { file, length: *length, offset });
            },

            Mode::Multiple { files: meta_files } => {
                // create root directory of torrent
                tokio::fs::create_dir_all(&base).await?;

                for f in meta_files {
                    let file_path = base.join(PathBuf::from_iter(&f.path));
                    if let Some(parent) = file_path.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    let file = OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .read(true)
                        .write(true)
                        .open(&file_path)
                        .await?;
                    file.set_len(f.length).await?;
                    files.push(OutputFile { file, length: f.length, offset });
                    offset += f.length;
                }
            },
        }

        Ok(files)
    }

    pub fn has_piece(&self, piece: u32) -> bool {
        self.bitfield.has_bit(piece as usize).unwrap()
    }

    pub fn missing_blocks(
        &self,
        piece: u32,
    ) -> impl Iterator<Item = BlockRange> + '_ {
        self.pieces[piece as usize]
            .blocks
            .iter()
            .enumerate()
            .filter(|(_, b)| match b {
                BlockState::Missing => true,
                BlockState::Requested { at } => {
                    at.elapsed() >= Duration::from_secs(60)
                },
                BlockState::Received { .. } => false,
            })
            .map(|(i, _)| BlockRange {
                begin: (i * BLOCK_SIZE),
                length: BLOCK_SIZE,
            })
    }

    pub fn mark_block_requested(&mut self, piece: u32, begin: usize) {
        let piece = &mut self.pieces[piece as usize];
        let index = begin / BLOCK_SIZE;

        if let Some(block) = piece.blocks.get_mut(index) {
            match block {
                BlockState::Missing => {
                    *block = BlockState::Requested { at: Instant::now() };
                },
                BlockState::Requested { at } => {
                    // allow retry if expired (same logic as missing_blocks)
                    if at.elapsed() >= Duration::from_secs(60) {
                        *block = BlockState::Requested { at: Instant::now() };
                    }
                },
                BlockState::Received { .. } => {
                    // do nothing (already completed)
                },
            }
        }
    }

    pub async fn write_block(
        &mut self,
        piece: u32,
        offset: usize,
        data: &[u8],
    ) -> Result<WriteResult> {
        let p = &mut self.pieces[piece as usize];
        let idx = offset / BLOCK_SIZE;

        // Ignore duplicates safely
        if matches!(p.blocks[idx], BlockState::Received { buffer: _ }) {
            return Ok(WriteResult::BlockStored);
        }

        p.blocks[idx] = BlockState::Received { buffer: data.to_vec() };
        p.received += 1;

        if p.received != p.blocks.len() {
            return Ok(WriteResult::BlockStored);
        }

        let buffer = p.buffer();
        let expected = self.metainfo.pieces[piece as usize];
        let verified = Self::verify_piece_hash(expected, &buffer);
        if !verified {
            p.reset();
            return Ok(WriteResult::PieceInvalid(piece));
        }

        // Write buffer to disk
        let piece_offset = piece as u64 * self.metainfo.piece_length;
        let piece_end = piece_offset + buffer.len() as u64;

        for file in &mut self.files {
            let file_start = file.offset;
            let file_end = file.offset + file.length;

            if piece_offset >= file_end || piece_end <= file_start {
                continue;
            }

            let write_start = piece_offset.max(file_start);
            let write_end = piece_end.min(file_end);

            let buffer_start = (write_start - piece_offset) as usize;
            let len = (write_end - write_start) as usize;

            file.file
                .seek(std::io::SeekFrom::Start(write_start - file_start))
                .await?;
            file.file
                .write_all(&buffer[buffer_start..buffer_start + len])
                .await?;
        }

        let _ = self.bitfield.set_bit(piece as usize);

        let ratio = self.bitfield.completion_ratio() * 100.0;
        println!("{ratio} %");

        Ok(WriteResult::PieceCompleted(piece))
    }

    fn verify_piece_hash(expected: [u8; 20], buffer: &Vec<u8>) -> bool {
        let digest = Sha1::digest(buffer);
        let mut actual = [0u8; 20];
        actual.copy_from_slice(&digest);
        actual == expected
    }
}
