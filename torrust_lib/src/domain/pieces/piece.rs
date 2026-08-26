use std::time::Instant;

pub(super) const BLOCK_SIZE: usize = 16 * 1024; // 16kb

#[derive(Clone)]
pub(super) enum BlockState {
    Missing,
    Requested { at: Instant },
    Received { buffer: Vec<u8> },
}

pub(super) struct Piece {
    pub(crate) blocks: Vec<BlockState>,
    pub(crate) received: usize,
}

impl Piece {
    pub fn new(piece_length: usize) -> Self {
        let num_blocks = piece_length.div_ceil(BLOCK_SIZE);
        Self {
            blocks: vec![BlockState::Missing; num_blocks],
            received: 0,
        }
    }

    pub fn missing_blocks(&self) -> impl Iterator<Item = usize> + '_ {
        self.blocks.iter().enumerate().filter_map(|(i, state)| {
            matches!(state, BlockState::Missing).then_some(i)
        })
    }

    pub fn mark_requested(&mut self, index: usize) {
        self.blocks[index] = BlockState::Requested { at: Instant::now() };
    }

    pub fn receive_block(&mut self, index: usize, data: Vec<u8>) {
        if let BlockState::Received { .. } = self.blocks[index] {
            return;
        }
        self.blocks[index] = BlockState::Received { buffer: data };
        self.received += 1;
    }

    pub fn is_complete(&self) -> bool {
        self.received == self.blocks.len()
    }

    pub fn buffer(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(self.blocks.len() * BLOCK_SIZE);
        for block in &self.blocks {
            if let BlockState::Received { buffer: block } = block {
                buffer.extend_from_slice(block);
            }
        }
        buffer
    }

    pub fn reset(&mut self) {
        for b in &mut self.blocks {
            *b = BlockState::Missing;
        }
        self.received = 0;
    }
}
