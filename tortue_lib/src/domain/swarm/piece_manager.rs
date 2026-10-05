use std::sync::Arc;

use sha1::{Digest, Sha1};

use crate::domain::{
    bitfield::{self, Bitfield},
    block::{BLOCK_SIZE, BlockRange},
    torrent::Metainfo,
};

#[derive(Debug, thiserror::Error)]
pub(super) enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("bitfield error: {0}")]
    Bitfield(#[from] bitfield::Error),

    #[error("invalid block size: expected {expected}, got {actual}")]
    InvalidBlockSize { expected: usize, actual: usize },

    #[error("invalid block index: {0}")]
    InvalidBlockIndex(usize),

    #[error("invalid piece index: {0}")]
    InvalidPieceIndex(usize),

    #[error("unaligned block offset: {0}")]
    UnalignedBlockOffset(usize),
}
pub(super) type Result<T> = std::result::Result<T, Error>;

pub(super) struct CompletedPiece {
    pub(super) range: BlockRange,
    pub(super) data: Vec<u8>,
}

pub(super) struct PieceManager {
    metainfo: Arc<Metainfo>,
    pieces: Vec<Piece>,
    bitfield: Bitfield,
}

impl PieceManager {
    pub(super) fn new(metainfo: Arc<Metainfo>) -> Self {
        let piece_count = metainfo.pieces.len();
        let mut pieces = Vec::with_capacity(piece_count);
        let bitfield = Bitfield::new(piece_count);

        for i in 0..piece_count {
            let piece_length = if i == piece_count - 1 {
                metainfo.total_size() as usize - i * metainfo.piece_length
            } else {
                metainfo.piece_length
            };
            pieces.push(Piece::new(
                *metainfo
                    .pieces
                    .get(i)
                    .expect("piece index within metainfo bounds"),
                piece_length,
            ));
        }

        Self {
            metainfo,
            pieces,
            bitfield,
        }
    }

    pub(super) fn bitfield(&self) -> Vec<u8> {
        self.bitfield.clone().into()
    }

    pub(super) fn unreceived_blocks(
        &self,
        piece_index: usize,
    ) -> impl Iterator<Item = BlockRange> + '_ {
        self.pieces
            .get(piece_index)
            .into_iter()
            .flat_map(move |piece| {
                piece
                    .unreceived_blocks()
                    .map(move |block_index| BlockRange {
                        piece_index,
                        piece_offset: block_index * BLOCK_SIZE,
                        len: piece.block_length(block_index).expect("iter on blocks"),
                    })
            })
    }

    pub(super) fn needed_pieces(&self) -> impl Iterator<Item = usize> + '_ {
        self.pieces
            .iter()
            .enumerate()
            .filter_map(|(i, p)| (!p.is_complete()).then_some(i))
    }

    pub(super) fn is_complete(&self) -> bool {
        self.pieces.iter().all(|p| p.is_complete())
    }

    pub(super) fn available_bytes(&self) -> u64 {
        self.pieces
            .iter()
            .filter(|piece| piece.is_complete())
            .map(|piece| piece.length as u64)
            .sum()
    }

    pub(super) fn is_partial(&self, piece_index: usize) -> bool {
        self.pieces.get(piece_index).is_some_and(Piece::is_partial)
    }

    pub(super) fn has_no_piece(&self) -> bool {
        self.pieces.iter().all(|p| !p.is_complete())
    }

    pub(super) fn blocks_total(&self) -> usize {
        self.pieces.iter().map(Piece::blocks_total).sum()
    }

    pub(super) fn blocks_received(&self) -> usize {
        self.pieces.iter().map(Piece::blocks_received).sum()
    }

    /// Returns a completed, hash-verified piece ready for storage.
    /// `Ok(None)` means a duplicate, an incomplete piece, or a hash mismatch.
    /// Only a hash mismatch resets the piece.
    pub(super) fn receive_block(
        &mut self,
        range: BlockRange,
        data: Vec<u8>,
    ) -> Result<Option<CompletedPiece>> {
        if data.len() != range.len {
            return Err(Error::InvalidBlockSize {
                expected: range.len,
                actual: data.len(),
            });
        }
        if !range.piece_offset.is_multiple_of(BLOCK_SIZE) {
            return Err(Error::UnalignedBlockOffset(range.piece_offset));
        }
        let piece_index = range.piece_index;
        let p = self
            .pieces
            .get_mut(piece_index)
            .ok_or(Error::InvalidPieceIndex(piece_index))?;
        let block_index = range.piece_offset / BLOCK_SIZE;
        let Some(buffer) = p.receive_block(block_index, data)? else {
            return Ok(None);
        };
        self.bitfield.set_bit(piece_index)?;
        Ok(Some(CompletedPiece {
            range: BlockRange {
                piece_index,
                piece_offset: 0,
                len: buffer.len(),
            },
            data: buffer,
        }))
    }

    pub(super) fn valid_upload_range(&self, range: BlockRange) -> bool {
        self.pieces.get(range.piece_index).is_some_and(|piece| {
            piece.is_complete()
                && range.len > 0
                && range.len <= BLOCK_SIZE
                && range
                    .piece_offset
                    .checked_add(range.len)
                    .is_some_and(|end| end <= piece.length)
        })
    }
}

fn verify_piece_hash(expected: [u8; 20], buffer: &[u8]) -> bool {
    let digest = Sha1::digest(buffer);
    digest.as_slice() == expected
}

struct Piece {
    length: usize,
    expected_hash: [u8; 20],
    state: PieceState,
}

// We do not store completed pieces in memory in order to save memory usage.
// Instead completed pieces data live on PieceStore and accessed via IO and managed
// by the Swarm / SwarmIO directly.
enum PieceState {
    Partial {
        blocks: Box<[BlockState]>,
        received: usize,
    },
    Complete,
}

#[derive(Clone)]
enum BlockState {
    Missing,
    Received(Vec<u8>),
}

impl Piece {
    fn new(expected_hash: [u8; 20], piece_length: usize) -> Self {
        let num_blocks = piece_length.div_ceil(BLOCK_SIZE);
        let blocks = vec![BlockState::Missing; num_blocks].into_boxed_slice();
        Self {
            length: piece_length,
            expected_hash,
            state: PieceState::Partial {
                blocks,
                received: 0,
            },
        }
    }

    fn unreceived_blocks(&self) -> impl Iterator<Item = usize> + '_ {
        let blocks: &[BlockState] = match &self.state {
            PieceState::Partial { blocks, .. } => blocks,
            PieceState::Complete => &[],
        };
        blocks
            .iter()
            .enumerate()
            .filter_map(|(index, state)| matches!(state, BlockState::Missing).then_some(index))
    }

    /// Returns the verified piece buffer on completion.
    /// Returns None for duplicates, incomplete pieces, or hash mismatch.
    fn receive_block(&mut self, block_index: usize, data: Vec<u8>) -> Result<Option<Vec<u8>>> {
        if matches!(&self.state, PieceState::Complete) {
            return Ok(None);
        }
        let expected_length = self.block_length(block_index)?;
        if data.len() != expected_length {
            return Err(Error::InvalidBlockSize {
                expected: expected_length,
                actual: data.len(),
            });
        }
        let PieceState::Partial { blocks, received } = &mut self.state else {
            unreachable!("complete piece handled above");
        };
        if matches!(blocks[block_index], BlockState::Received(_)) {
            return Ok(None);
        }
        blocks[block_index] = BlockState::Received(data);
        *received += 1;
        let Some(buffer) = self.buffer() else {
            return Ok(None);
        };
        if !verify_piece_hash(self.expected_hash, &buffer) {
            self.reset();
            return Ok(None);
        }
        self.state = PieceState::Complete;
        Ok(Some(buffer))
    }

    fn block_length(&self, block_index: usize) -> Result<usize> {
        match &self.state {
            PieceState::Complete => Ok(0),
            PieceState::Partial { blocks, .. } => {
                if block_index >= blocks.len() {
                    return Err(Error::InvalidBlockIndex(block_index));
                }
                let offset = block_index * BLOCK_SIZE;
                Ok((self.length - offset).min(BLOCK_SIZE))
            },
        }
    }

    fn blocks_total(&self) -> usize {
        self.length.div_ceil(BLOCK_SIZE)
    }

    fn blocks_received(&self) -> usize {
        match &self.state {
            PieceState::Partial { received, .. } => *received,
            PieceState::Complete => self.blocks_total(),
        }
    }

    fn is_complete(&self) -> bool {
        match self.state {
            PieceState::Partial { .. } => false,
            PieceState::Complete => true,
        }
    }

    fn is_partial(&self) -> bool {
        match self.state {
            PieceState::Partial { received, .. } => received > 0,
            PieceState::Complete => false,
        }
    }

    fn buffer(&self) -> Option<Vec<u8>> {
        let PieceState::Partial { blocks, received } = &self.state else {
            return None;
        };
        if *received != blocks.len() {
            return None;
        }
        let mut buffer = Vec::with_capacity(self.length);
        for block in blocks {
            let BlockState::Received(data) = block else {
                unreachable!("all blocks received but a block is missing");
            };
            buffer.extend_from_slice(data);
        }
        Some(buffer)
    }

    fn reset(&mut self) {
        match &mut self.state {
            PieceState::Complete => {},
            PieceState::Partial { blocks, received } => {
                for block in blocks {
                    *block = BlockState::Missing;
                }
                *received = 0;
            },
        }
    }
}
