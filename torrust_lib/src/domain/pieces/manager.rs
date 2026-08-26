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
