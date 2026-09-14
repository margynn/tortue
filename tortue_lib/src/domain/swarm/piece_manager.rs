use std::sync::Arc;

use sha1::{Digest, Sha1};

use crate::domain::{
    bitfield::{self, Bitfield},
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
}
pub(super) type Result<T> = std::result::Result<T, Error>;

const BLOCK_SIZE: usize = 16 * 1024; // 16 KiB

pub(super) struct CompletedPiece {
    pub(super) piece_index: usize,
    pub(super) piece_offset: u64,
    pub(super) data: Vec<u8>,
}

pub(super) struct PieceManager {
    metainfo: Arc<Metainfo>,
    pieces: Vec<Piece>,
    bitfield: Bitfield,

    pub(super) uploaded_bytes: usize,
    pub(super) downloaded_bytes: usize,
}

#[derive(Clone, Copy)]
pub(super) struct BlockRange {
    pub(super) block: BlockRef,
    pub(super) len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct BlockRef {
    pub(super) piece_index: usize,
    pub(super) piece_offset: usize,
}

impl BlockRef {
    fn block_index(&self) -> usize {
        self.piece_offset / BLOCK_SIZE
    }
}

impl PieceManager {
    // TODO: should initialize with existing content when available
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
            pieces.push(Piece::new(piece_length));
        }

        Self {
            metainfo,
            pieces,
            bitfield,
            uploaded_bytes: 0,
            downloaded_bytes: 0,
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
                        block: BlockRef {
                            piece_index,
                            piece_offset: block_index * BLOCK_SIZE,
                        },
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

    pub(super) fn has_no_piece(&self) -> bool {
        self.pieces.iter().all(|p| !p.is_complete())
    }

    pub(super) fn read_block(
        &mut self,
        piece_index: usize,
        piece_offset: usize,
        piece_len: usize,
    ) -> Option<Vec<u8>> {
        if piece_len > BLOCK_SIZE {
            return None;
        }
        self.uploaded_bytes += piece_len;
        self.pieces.get(piece_index)?.read(piece_offset, piece_len)
    }

    pub(super) fn blocks_total(&self) -> usize {
        self.pieces.iter().map(|p| p.blocks.len()).sum()
    }

    pub(super) fn blocks_received(&self) -> usize {
        self.pieces.iter().map(|p| p.received).sum()
    }

    /// `Ok(None)` covers both "block received, piece still incomplete" and
    /// "piece completed but failed its hash" (reset internally either way) —
    /// nothing downstream distinguishes them today.
    pub(super) fn receive_block(
        &mut self,
        block_ref: BlockRef,
        data: Vec<u8>,
    ) -> Result<Option<CompletedPiece>> {
        let piece_index = block_ref.piece_index;
        let p = self
            .pieces
            .get_mut(piece_index)
            .ok_or(Error::InvalidPieceIndex(piece_index))?;
        self.downloaded_bytes += data.len();

        // An endgame duplicate must not re-emit a completion: that would
        // write the piece and broadcast `Have` twice.
        if !p.receive_block(block_ref.block_index(), data)? || !p.is_complete() {
            return Ok(None);
        }

        let buffer = p.buffer().expect("piece is complete");
        let expected_hash = self.metainfo.pieces[piece_index];

        if !verify_piece_hash(expected_hash, &buffer) {
            p.reset();
            return Ok(None);
        }

        let torrent_offset = piece_index as u64 * self.metainfo.piece_length as u64;
        self.bitfield.set_bit(piece_index)?;
        Ok(Some(CompletedPiece {
            piece_index,
            piece_offset: torrent_offset,
            data: buffer,
        }))
    }
}

fn verify_piece_hash(expected: [u8; 20], buffer: &[u8]) -> bool {
    let digest = Sha1::digest(buffer);
    digest.as_slice() == expected
}

#[derive(Clone)]
enum BlockState {
    Missing,
    Received(Vec<u8>),
}

struct Piece {
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

    fn unreceived_blocks(&self) -> impl Iterator<Item = usize> + '_ {
        self.blocks
            .iter()
            .enumerate()
            .filter_map(|(index, state)| matches!(state, BlockState::Missing).then_some(index))
    }

    /// `false` when the block was already received, i.e. nothing changed.
    fn receive_block(&mut self, block_index: usize, data: Vec<u8>) -> Result<bool> {
        let expected_length = self.block_length(block_index)?;
        if data.len() != expected_length {
            return Err(Error::InvalidBlockSize {
                expected: expected_length,
                actual: data.len(),
            });
        }
        if matches!(self.blocks[block_index], BlockState::Received(_)) {
            return Ok(false);
        }
        self.blocks[block_index] = BlockState::Received(data);
        self.received += 1;
        Ok(true)
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
            let BlockState::Received(data) = block else {
                unreachable!("complete piece contains a non-received block");
            };
            buffer.extend_from_slice(data);
        }
        Some(buffer)
    }

    fn read(&self, offset: usize, len: usize) -> Option<Vec<u8>> {
        if offset.checked_add(len)? > self.length {
            return None;
        }
        let mut out = Vec::with_capacity(len);
        let mut pos = offset;
        while out.len() < len {
            let block_index = pos / BLOCK_SIZE;
            let within_block = pos % BLOCK_SIZE;
            let BlockState::Received(buffer) = self.blocks.get(block_index)? else {
                return None;
            };
            let take = (len - out.len()).min(buffer.len() - within_block);
            out.extend_from_slice(&buffer[within_block..within_block + take]);
            pos += take;
        }
        Some(out)
    }

    fn reset(&mut self) {
        for block in &mut self.blocks {
            *block = BlockState::Missing;
        }
        self.received = 0;
    }
}
