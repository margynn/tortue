use std::future::Future;
use std::{fmt, io};

use super::{Error, Result};

const MAX_MESSAGE_SIZE: usize = 1 << 20;

pub trait AsyncByteReader {
    fn read_exact<'a>(&'a mut self, buf: &'a mut [u8])
    -> impl Future<Output = io::Result<()>> + 'a;
}

#[derive(Clone)]
pub enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Vec<u8>),
    Request { index: u32, begin: u32, length: u32 },
    Piece { index: u32, begin: u32, block: Vec<u8> },
    Cancel { index: u32, begin: u32, length: u32 },
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Piece { index, begin, block } => f
                .debug_struct("Piece")
                .field("index", index)
                .field("begin", begin)
                .field("size", &block.len())
                .finish(),
            Self::Bitfield(bits) => {
                write!(f, "Bitfield({} bytes)", bits.len())
            },
            Self::KeepAlive => write!(f, "KeepAlive"),
            Self::Choke => write!(f, "Choke"),
            Self::Unchoke => write!(f, "Unchoke"),
            Self::Interested => write!(f, "Interested"),
            Self::NotInterested => write!(f, "NotInterested"),
            Self::Have(piece) => write!(f, "Have({piece})"),
            Self::Request { index, begin, length } => f
                .debug_struct("Request")
                .field("index", index)
                .field("begin", begin)
                .field("length", length)
                .finish(),
            Self::Cancel { index, begin, length } => f
                .debug_struct("Cancel")
                .field("index", index)
                .field("begin", begin)
                .field("length", length)
                .finish(),
        }
    }
}

impl Message {
    pub async fn read_from<R: AsyncByteReader>(reader: &mut R) -> Result<Self> {
        let mut header = [0u8; 4];
        reader.read_exact(&mut header).await.map_err(|_| Error::Io)?;
        let len = Self::parse_frame_len(header)?;

        let mut payload = vec![0u8; len];
        reader.read_exact(&mut payload).await.map_err(|_| Error::Io)?;

        Self::decode_framed(&payload)
    }

    fn parse_frame_len(header: [u8; 4]) -> Result<usize> {
        let len = u32::from_be_bytes(header) as usize;
        if len > MAX_MESSAGE_SIZE {
            return Err(Error::MessageTooLarge);
        }
        Ok(len)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Message Framing: <length prefix><message>, length prefix is 4-byte big-endian
        match self {
            Message::KeepAlive => buf.extend_from_slice(&0u32.to_be_bytes()),
            Message::Choke => buf.extend_from_slice(&[0, 0, 0, 1, 0]),
            Message::Unchoke => buf.extend_from_slice(&[0, 0, 0, 1, 1]),
            Message::Interested => buf.extend_from_slice(&[0, 0, 0, 1, 2]),
            Message::NotInterested => buf.extend_from_slice(&[0, 0, 0, 1, 3]),
            Message::Have(piece) => {
                buf.extend_from_slice(&5u32.to_be_bytes());
                buf.push(4);
                buf.extend_from_slice(&piece.to_be_bytes());
            },
            Message::Bitfield(bits) => {
                buf.extend_from_slice(&(1 + bits.len() as u32).to_be_bytes());
                buf.push(5);
                buf.extend_from_slice(bits);
            },
            Message::Request { index, begin, length } => {
                buf.extend_from_slice(&13u32.to_be_bytes());
                buf.push(6);
                buf.extend_from_slice(&index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());
            },
            Message::Piece { index, begin, block } => {
                buf.extend_from_slice(&(9 + block.len() as u32).to_be_bytes());
                buf.push(7);
                buf.extend_from_slice(&index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(block);
            },
            Message::Cancel { index, begin, length } => {
                buf.extend_from_slice(&13u32.to_be_bytes());
                buf.push(8);
                buf.extend_from_slice(&index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());
            },
        }

        buf
    }

    pub fn decode_framed(data: &[u8]) -> Result<Message> {
        if data.is_empty() {
            return Ok(Message::KeepAlive);
        }

        let msg_id = data[0];
        let payload = &data[1..];

        match msg_id {
            0 => Ok(Message::Choke),
            1 => Ok(Message::Unchoke),
            2 => Ok(Message::Interested),
            3 => Ok(Message::NotInterested),
            4 => {
                if payload.len() != 4 {
                    return Err(Error::InvalidMessage);
                }
                let piece =
                    u32::from_be_bytes(payload.try_into().map_err(|_| Error::InvalidMessage)?);
                Ok(Message::Have(piece))
            },
            5 => Ok(Message::Bitfield(payload.to_vec())),
            6 => {
                if payload.len() != 12 {
                    return Err(Error::InvalidMessage);
                }
                let index = u32::from_be_bytes(
                    payload[0..4].try_into().map_err(|_| Error::InvalidMessage)?,
                );
                let begin = u32::from_be_bytes(
                    payload[4..8].try_into().map_err(|_| Error::InvalidMessage)?,
                );
                let length = u32::from_be_bytes(
                    payload[8..12].try_into().map_err(|_| Error::InvalidMessage)?,
                );
                Ok(Message::Request { index, begin, length })
            },
            7 => {
                if payload.len() < 8 {
                    return Err(Error::InvalidMessage);
                }
                let index = u32::from_be_bytes(
                    payload[0..4].try_into().map_err(|_| Error::InvalidMessage)?,
                );
                let begin = u32::from_be_bytes(
                    payload[4..8].try_into().map_err(|_| Error::InvalidMessage)?,
                );
                let block = payload[8..].to_vec();
                Ok(Message::Piece { index, begin, block })
            },
            8 => {
                if payload.len() != 12 {
                    return Err(Error::InvalidMessage);
                }
                let index = u32::from_be_bytes(
                    payload[0..4].try_into().map_err(|_| Error::InvalidMessage)?,
                );
                let begin = u32::from_be_bytes(
                    payload[4..8].try_into().map_err(|_| Error::InvalidMessage)?,
                );
                let length = u32::from_be_bytes(
                    payload[8..12].try_into().map_err(|_| Error::InvalidMessage)?,
                );
                Ok(Message::Cancel { index, begin, length })
            },
            _ => Err(Error::InvalidMessage),
        }
    }
}
