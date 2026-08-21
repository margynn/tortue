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
