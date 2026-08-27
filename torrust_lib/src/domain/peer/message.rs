use super::{Error, Result};

#[derive(Debug, Clone)]
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

impl Message {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        // Message Framing:
        //      <length prefix><message>
        //      Length prefix: 4-byte big-endian

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
                let len = 1u32
                    .checked_add(bits.len() as u32)
                    .ok_or(Error::InvalidMessage)?;
                buf.extend_from_slice(&len.to_be_bytes());
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
                let len = 9u32
                    .checked_add(block.len() as u32)
                    .ok_or(Error::InvalidMessage)?;
                buf.extend_from_slice(&len.to_be_bytes());
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

        Ok(buf)
    }

    pub fn decode(data: &[u8]) -> Result<Message> {
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
                let piece = u32::from_be_bytes(
                    payload.try_into().map_err(|_| Error::InvalidMessage)?,
                );
                Ok(Message::Have(piece))
            },
            5 => Ok(Message::Bitfield(payload.to_vec())),
            6 => {
                if payload.len() != 12 {
                    return Err(Error::InvalidMessage);
                }
                let index = u32::from_be_bytes(
                    payload[0..4]
                        .try_into()
                        .map_err(|_| Error::InvalidMessage)?,
                );
                let begin = u32::from_be_bytes(
                    payload[4..8]
                        .try_into()
                        .map_err(|_| Error::InvalidMessage)?,
                );
                let length = u32::from_be_bytes(
                    payload[8..12]
                        .try_into()
                        .map_err(|_| Error::InvalidMessage)?,
                );
                Ok(Message::Request { index, begin, length })
            },
            7 => {
                if payload.len() < 8 {
                    return Err(Error::InvalidMessage);
                }
                let index = u32::from_be_bytes(
                    payload[0..4]
                        .try_into()
                        .map_err(|_| Error::InvalidMessage)?,
                );
                let begin = u32::from_be_bytes(
                    payload[4..8]
                        .try_into()
                        .map_err(|_| Error::InvalidMessage)?,
                );
                let block = payload[8..].to_vec();
                Ok(Message::Piece { index, begin, block })
            },
            8 => {
                if payload.len() != 12 {
                    return Err(Error::InvalidMessage);
                }
                let index = u32::from_be_bytes(
                    payload[0..4]
                        .try_into()
                        .map_err(|_| Error::InvalidMessage)?,
                );
                let begin = u32::from_be_bytes(
                    payload[4..8]
                        .try_into()
                        .map_err(|_| Error::InvalidMessage)?,
                );
                let length = u32::from_be_bytes(
                    payload[8..12]
                        .try_into()
                        .map_err(|_| Error::InvalidMessage)?,
                );
                Ok(Message::Cancel { index, begin, length })
            },
            _ => Err(Error::InvalidMessage),
        }
    }
}
