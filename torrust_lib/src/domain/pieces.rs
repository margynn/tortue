use std::time::{Duration, Instant};

use sha1::{Digest, Sha1};

use crate::domain::torrent::Metainfo;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid block size: expected {expected}, got {actual}")]
    InvalidBlockSize { expected: usize, actual: usize },

    #[error("invalid block index: {0}")]
    InvalidBlockIndex(usize),
}
pub type Result<T> = std::result::Result<T, Error>;

const BLOCK_SIZE: usize = 16 * 1024; // 16 KiB
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub enum PieceEvent {
    BlockReceived,
    PieceCompleted {
        piece_index: usize,
        piece_offset: u64,
        data: Vec<u8>,
    },
    PieceInvalid {
        piece_index: usize,
    },
}

pub struct PieceManager<'a> {
    metainfo: &'a Metainfo,
    pieces: Vec<Piece>,
}

pub struct BlockRange {
    pub piece_index: usize,
    pub piece_offset: usize,
    pub piece_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockRef {
    pub piece_index: usize,
    pub piece_offset: usize,
}

impl From<&BlockRange> for BlockRef {
    fn from(b: &BlockRange) -> Self {
        Self {
            piece_index: b.piece_index,
            piece_offset: b.piece_offset,
        }
    }
}

impl<'a> PieceManager<'a> {
    pub fn new(metainfo: &'a Metainfo) -> Self {
        let piece_count = metainfo.pieces.len();
        let mut pieces = Vec::with_capacity(piece_count);

        for i in 0..piece_count {
            let piece_length = if i == piece_count - 1 {
                metainfo.total_size() as usize - i * metainfo.piece_length
            } else {
                metainfo.piece_length
            };
            pieces.push(Piece::new(piece_length));
        }

        Self { metainfo, pieces }
    }

    pub fn missing_blocks(&self, piece_index: usize) -> impl Iterator<Item = BlockRange> + '_ {
        let piece = &self.pieces[piece_index];
        piece.missing_blocks().map(move |block_index| BlockRange {
            piece_index,
            piece_offset: block_index * BLOCK_SIZE,
            piece_len: piece.block_length(block_index).expect("iter on blocks"),
        })
    }

    pub fn request_block(&mut self, block_ref: BlockRef) -> Result<()> {
        let block_index = block_ref.piece_offset / BLOCK_SIZE;
        self.pieces[block_ref.piece_index].request_block(block_index)
    }

    pub fn needed_pieces(&self) -> impl Iterator<Item = usize> + '_ {
        self.pieces
            .iter()
            .enumerate()
            .filter_map(|(i, p)| (!p.is_complete()).then_some(i))
    }

    pub fn is_complete(&self) -> bool {
        self.pieces.iter().all(|p| p.is_complete())
    }

    pub fn receive_block(&mut self, block_ref: BlockRef, data: Vec<u8>) -> Result<PieceEvent> {
        let piece_index = block_ref.piece_index;
        let block_index = block_ref.piece_offset / BLOCK_SIZE;
        let p = &mut self.pieces[piece_index];

        p.receive_block(block_index, data)?;

        if !p.is_complete() {
            return Ok(PieceEvent::BlockReceived);
        }

        let buffer = p.buffer().expect("piece is complete");
        let expected_hash = self.metainfo.pieces[piece_index];

        if !verify_piece_hash(expected_hash, &buffer) {
            p.reset();
            return Ok(PieceEvent::PieceInvalid { piece_index });
        }

        let torrent_offset = piece_index as u64 * self.metainfo.piece_length as u64;

        Ok(PieceEvent::PieceCompleted {
            piece_index,
            piece_offset: torrent_offset,
            data: buffer,
        })
    }
}

fn verify_piece_hash(expected: [u8; 20], buffer: &[u8]) -> bool {
    let digest = Sha1::digest(buffer);
    digest.as_slice() == expected
}

#[derive(Clone)]
pub enum BlockState {
    Missing,
    Requested { at: Instant },
    Received { buffer: Vec<u8> },
}

pub struct Piece {
    blocks: Vec<BlockState>,
    length: usize,
    received: usize,
}

impl Piece {
    fn new(piece_length: usize) -> Self {
        let num_blocks = piece_length.div_ceil(BLOCK_SIZE);
        Self {
            blocks: vec![BlockState::Missing; num_blocks],
            length: piece_length,
            received: 0,
        }
    }

    fn missing_blocks(&self) -> impl Iterator<Item = usize> + '_ {
        let now = Instant::now();
        self.blocks.iter().enumerate().filter_map(move |(index, state)| {
            let requestable = match state {
                BlockState::Missing => true,
                BlockState::Requested { at } => now >= *at + REQUEST_TIMEOUT,
                BlockState::Received { .. } => false,
            };
            requestable.then_some(index)
        })
    }

    fn request_block(&mut self, block_index: usize) -> Result<()> {
        if block_index >= self.blocks.len() {
            return Err(Error::InvalidBlockIndex(block_index));
        }
        self.blocks[block_index] = BlockState::Requested { at: Instant::now() };
        Ok(())
    }

    fn receive_block(&mut self, block_index: usize, data: Vec<u8>) -> Result<()> {
        let expected_length = self.block_length(block_index)?;
        if data.len() != expected_length {
            return Err(Error::InvalidBlockSize {
                expected: expected_length,
                actual: data.len(),
            });
        }
        // Prevent duplicated blocks
        if matches!(self.blocks[block_index], BlockState::Received { .. }) {
            return Ok(());
        }
        self.blocks[block_index] = BlockState::Received { buffer: data };
        self.received += 1;
        Ok(())
    }

    fn block_length(&self, block_index: usize) -> Result<usize> {
        if block_index >= self.blocks.len() {
            return Err(Error::InvalidBlockIndex(block_index));
        }
        let offset = block_index * BLOCK_SIZE;
        Ok((self.length - offset).min(BLOCK_SIZE))
    }

    fn is_complete(&self) -> bool {
        self.received == self.blocks.len()
    }

    fn buffer(&self) -> Option<Vec<u8>> {
        if !self.is_complete() {
            return None;
        }
        let mut buffer = Vec::with_capacity(self.length);
        for block in &self.blocks {
            let BlockState::Received { buffer: data } = block else {
                unreachable!("complete piece contains a non-received block");
            };
            buffer.extend_from_slice(data);
        }
        Some(buffer)
    }

    fn reset(&mut self) {
        for block in &mut self.blocks {
            *block = BlockState::Missing;
        }
        self.received = 0;
    }
}
