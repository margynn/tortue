use std::path::PathBuf;
use std::time::Instant;

use sha1::{Digest, Sha1};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::metainfo::Metainfo;
use crate::peer::PeerId;
use crate::peer::bitfield::Bitfield;

const BLOCK_SIZE: u32 = 16 * 1024; // 16kb

#[derive(Clone, Copy, Debug)]
enum BlockState {
    Missing,
    Reserved { peer: PeerId, at: Instant },
    Received,
}

#[derive(Debug)]
struct Piece {
    blocks: Vec<BlockState>,
    buffer: Vec<u8>,
    received: usize,
}

#[derive(Debug)]
pub enum WriteResult {
    BlockStored,
    PieceCompleted(u32),
    PieceInvalid(u32),
}

pub struct PieceManager {
    metainfo: Metainfo,
    bitfield: Bitfield,
    pieces: Vec<Piece>,
    file: File,
}

impl PieceManager {
    pub async fn new(
        metainfo: Metainfo,
        path: PathBuf,
    ) -> std::io::Result<Self> {
        let total_size = metainfo.size();

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .await?;

        file.set_len(total_size).await?;

        let mut pieces = Vec::with_capacity(metainfo.pieces.len());

        for (i, _) in metainfo.pieces.iter().enumerate() {
            let piece_len = Self::piece_length(&metainfo, i);

            let blocks = (piece_len as u32 + BLOCK_SIZE - 1) / BLOCK_SIZE;

            pieces.push(Piece {
                blocks: vec![BlockState::Missing; blocks as usize],
                buffer: vec![0; piece_len as usize],
                received: 0,
            });
        }

        Ok(Self {
            bitfield: Bitfield::new(metainfo.pieces.len()),
            metainfo,
            pieces,
            file,
        })
    }

    fn piece_length(meta: &Metainfo, index: usize) -> u64 {
        let last = meta.pieces.len() - 1;
        if index == last {
            meta.size() - (meta.piece_length * index as u64)
        } else {
            meta.piece_length
        }
    }

    fn block_index(offset: u32) -> usize {
        (offset / BLOCK_SIZE) as usize
    }

    // -------------------------
    // Queries
    // -------------------------

    pub fn bitfield(&self) -> &Bitfield {
        &self.bitfield
    }

    pub fn has_piece(&self, piece: u32) -> bool {
        self.bitfield.has_bit(piece as usize).unwrap()
    }

    pub fn is_block_needed(&self, piece: u32, offset: u32) -> bool {
        let p = &self.pieces[piece as usize];
        let idx = Self::block_index(offset);
        matches!(p.blocks[idx], BlockState::Missing)
    }

    pub fn missing_blocks(&self, piece: u32) -> impl Iterator<Item = u32> + '_ {
        self.pieces[piece as usize]
            .blocks
            .iter()
            .enumerate()
            .filter(|(_, b)| matches!(b, BlockState::Missing))
            .map(|(i, _)| i as u32)
    }

    // -------------------------
    // Reservation
    // -------------------------

    pub fn reserve_block(
        &mut self,
        piece: u32,
        offset: u32,
        peer: PeerId,
    ) -> bool {
        let p = &mut self.pieces[piece as usize];
        let idx = Self::block_index(offset);

        match p.blocks[idx] {
            BlockState::Missing => {
                p.blocks[idx] =
                    BlockState::Reserved { peer, at: Instant::now() };
                true
            },
            _ => false,
        }
    }

    pub fn cancel_block(&mut self, piece: u32, offset: u32) {
        let p = &mut self.pieces[piece as usize];
        let idx = Self::block_index(offset);

        if matches!(p.blocks[idx], BlockState::Reserved { .. }) {
            p.blocks[idx] = BlockState::Missing;
        }
    }

    // -------------------------
    // Write path
    // -------------------------

    pub async fn write_block(
        &mut self,
        piece: u32,
        offset: u32,
        data: &[u8],
    ) -> std::io::Result<WriteResult> {
        let p = &mut self.pieces[piece as usize];
        let idx = Self::block_index(offset);

        // Ignore duplicates safely
        if matches!(p.blocks[idx], BlockState::Received) {
            return Ok(WriteResult::BlockStored);
        }

        // Copy into piece buffer
        let start = offset as usize;
        let end = start + data.len();
        p.buffer[start..end].copy_from_slice(data);

        p.blocks[idx] = BlockState::Received;
        p.received += 1;

        if p.received != p.blocks.len() {
            return Ok(WriteResult::BlockStored);
        }

        // Piece complete → verify hash
        let expected = self.metainfo.pieces[piece as usize];
        let digest = Sha1::digest(p.buffer.clone());
        let mut actual = [0u8; 20];
        actual.copy_from_slice(&digest);

        if actual != expected {
            // reset
            for b in &mut p.blocks {
                *b = BlockState::Missing;
            }
            p.received = 0;
            return Ok(WriteResult::PieceInvalid(piece));
        }

        // Write to disk
        let global_offset = piece as u64 * self.metainfo.piece_length;

        self.file.seek(std::io::SeekFrom::Start(global_offset)).await?;
        self.file.write_all(&p.buffer).await?;

        let _ = self.bitfield.set_bit(piece as usize);

        Ok(WriteResult::PieceCompleted(piece))
    }

    // -------------------------
    // Read path (upload)
    // -------------------------

    pub async fn read_block(
        &mut self,
        piece: u32,
        offset: u32,
        length: u32,
    ) -> std::io::Result<Vec<u8>> {
        if !self.has_piece(piece) {
            return Ok(Vec::new());
        }

        let mut buf = vec![0; length as usize];

        let global_offset =
            piece as u64 * self.metainfo.piece_length + offset as u64;

        self.file.seek(std::io::SeekFrom::Start(global_offset)).await?;

        self.file.read_exact(&mut buf).await?;

        Ok(buf)
    }

    // -------------------------
    // Completion
    // -------------------------

    // pub fn is_complete(&self) -> bool {
    //     self.bitfield.all()
    // }
}
