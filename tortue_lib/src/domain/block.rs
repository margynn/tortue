pub(crate) const BLOCK_SIZE: usize = 16 * 1024; // 16 KiB

/// An interval within a piece; upload offsets need not be aligned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockRange {
    pub piece_index: usize,
    pub piece_offset: usize,
    pub len: usize,
}
