use std::time::{Duration, Instant};

pub const BLOCK_SIZE: usize = 16 * 1024; // 16 KiB
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, thiserror::Error)]
pub enum PieceError {
    #[error("invalid block size: expected {expected}, got {actual}")]
    InvalidBlockSize { expected: usize, actual: usize },

    #[error("invalid block index: {0}")]
    InvalidBlockIndex(usize),
}
pub type Result<T> = std::result::Result<T, PieceError>;

#[derive(Clone, Debug)]
pub enum BlockState {
    Missing,
    Requested { at: Instant },
    Received { buffer: Vec<u8> },
}

#[derive(Debug)]
pub struct Piece {
    blocks: Vec<BlockState>,
    length: usize,
    received: usize,
}

impl Piece {
    pub fn new(piece_length: usize) -> Self {
        let num_blocks = piece_length.div_ceil(BLOCK_SIZE);
        Self {
            blocks: vec![BlockState::Missing; num_blocks],
            length: piece_length,
            received: 0,
        }
    }

    pub fn missing_blocks(&self) -> impl Iterator<Item = usize> + '_ {
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

    pub fn request_block(&mut self, block_index: usize) -> Result<()> {
        if block_index >= self.blocks.len() {
            return Err(PieceError::InvalidBlockIndex(block_index));
        }
        self.blocks[block_index] = BlockState::Requested { at: Instant::now() };
        Ok(())
    }

    pub fn receive_block(&mut self, block_index: usize, data: Vec<u8>) -> Result<()> {
        let expected_length = self.block_length(block_index)?;
        if data.len() != expected_length {
            return Err(PieceError::InvalidBlockSize {
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
            return Err(PieceError::InvalidBlockIndex(block_index));
        }
        let offset = block_index * BLOCK_SIZE;
        Ok((self.length - offset).min(BLOCK_SIZE))
    }

    pub fn is_complete(&self) -> bool {
        self.received == self.blocks.len()
    }

    pub fn buffer(&self) -> Option<Vec<u8>> {
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

    pub fn reset(&mut self) {
        for block in &mut self.blocks {
            *block = BlockState::Missing;
        }
        self.received = 0;
    }
}
