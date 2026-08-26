use std::path::PathBuf;
use std::time::{Duration, Instant};

use sha1::{Digest, Sha1};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use super::errors::Result;
use super::piece::{BLOCK_SIZE, BlockState, Piece};
use crate::peer::bitfield::Bitfield;
use crate::torrent::{Metainfo, Mode};

// ============================================================================
// Storage abstraction
// ============================================================================

#[derive(Debug)]
pub enum StorageCommand {
    Write { offset: u64, data: Vec<u8> },
}

#[derive(Debug)]
pub struct StorageError {
    // implementation à faire
}

pub trait Storage {
    type Error;

    async fn execute(
        &mut self,
        command: StorageCommand,
    ) -> Result<(), Self::Error>;
}

// ============================================================================
// Tokio storage implementation
// ============================================================================

struct OutputFile {
    file: File,
    length: u64,
    offset: u64,
}

pub struct TokioStorage {
    files: Vec<OutputFile>,
}

impl TokioStorage {
    pub async fn new(metainfo: &Metainfo, root: PathBuf) -> Result<Self> {
        let files = Self::create_files(metainfo, root).await?;

        Ok(Self { files })
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

    async fn write(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        let write_end = offset + data.len() as u64;

        for file in &mut self.files {
            let file_start = file.offset;
            let file_end = file.offset + file.length;

            if offset >= file_end || write_end <= file_start {
                continue;
            }

            let write_start = offset.max(file_start);
            let write_end = write_end.min(file_end);

            let buffer_start = (write_start - offset) as usize;

            let len = (write_end - write_start) as usize;

            file.file
                .seek(std::io::SeekFrom::Start(write_start - file_start))
                .await?;

            file.file
                .write_all(&data[buffer_start..buffer_start + len])
                .await?;
        }

        Ok(())
    }
}

impl Storage for TokioStorage {
    type Error = std::io::Error;

    async fn execute(
        &mut self,
        command: StorageCommand,
    ) -> Result<(), Self::Error> {
        match command {
            StorageCommand::Write { offset, data } => {
                self.write(offset, &data).await
            },
        }
    }
}

// ============================================================================
// Piece manager
// ============================================================================

#[derive(Debug)]
pub enum PieceEvent {
    BlockReceived,

    PieceCompleted { piece: u32, command: StorageCommand },

    PieceInvalid { piece: u32 },
}

pub struct PieceManager {
    metainfo: Metainfo,
    bitfield: Bitfield,
    pieces: Vec<Piece>,
}

pub struct BlockRange {
    pub begin: usize,
    pub length: usize,
}

impl PieceManager {
    pub fn new(metainfo: Metainfo) -> Self {
        let mut pieces = Vec::with_capacity(metainfo.pieces.len());

        for (i, _) in metainfo.pieces.iter().enumerate() {
            let is_last = i == metainfo.pieces.len() - 1;

            let piece_length = if is_last {
                metainfo.size() - metainfo.piece_length * i as u64
            } else {
                metainfo.piece_length
            };

            pieces.push(Piece::new(piece_length as usize));
        }

        Self {
            bitfield: Bitfield::new(metainfo.pieces.len()),
            metainfo,
            pieces,
        }
    }

    pub fn has_piece(&self, piece: u32) -> bool {
        self.bitfield.has_bit(piece as usize).unwrap()
    }

    pub fn missing_blocks(
        &self,
        piece: u32,
    ) -> impl Iterator<Item = BlockRange> + '_ {
        self.pieces[piece as usize].missing_blocks().map(|index| BlockRange {
            begin: index * BLOCK_SIZE,
            length: BLOCK_SIZE,
        })
    }

    pub fn mark_block_requested(&mut self, piece: u32, begin: usize) {
        let index = begin / BLOCK_SIZE;

        self.pieces[piece as usize].mark_requested(index);
    }

    pub fn receive_block(
        &mut self,
        piece: u32,
        offset: usize,
        data: Vec<u8>,
    ) -> Result<PieceEvent> {
        let p = &mut self.pieces[piece as usize];

        let index = offset / BLOCK_SIZE;

        p.receive_block(index, data);

        if !p.is_complete() {
            return Ok(PieceEvent::BlockReceived);
        }

        let buffer = p.buffer();

        let expected = self.metainfo.pieces[piece as usize];

        if !Self::verify_piece_hash(expected, &buffer) {
            p.reset();

            return Ok(PieceEvent::PieceInvalid { piece });
        }

        let piece_offset = piece as u64 * self.metainfo.piece_length;

        Ok(PieceEvent::PieceCompleted {
            piece,
            command: StorageCommand::Write {
                offset: piece_offset,
                data: buffer,
            },
        })
    }

    pub fn storage_completed(&mut self, piece: u32) {
        self.bitfield.set_bit(piece as usize).unwrap();
    }

    fn verify_piece_hash(expected: [u8; 20], buffer: &[u8]) -> bool {
        let digest = Sha1::digest(buffer);

        digest.as_slice() == expected
    }
}

// APPLICATION CODE LATTER

pub async fn handle_block(
    piece_manager: &mut PieceManager,
    storage: &mut TokioStorage,
    piece: u32,
    offset: usize,
    data: Vec<u8>,
) -> Result<()> {
    let event = piece_manager.receive_block(piece, offset, data)?;

    match event {
        PieceEvent::BlockReceived => {},

        PieceEvent::PieceInvalid { piece } => {
            println!("piece {piece} invalid");
        },

        PieceEvent::PieceCompleted { piece, command } => {
            storage.execute(command).await?;

            piece_manager.storage_completed(piece);
        },
    }

    Ok(())
}
