use std::{collections::HashMap, fmt};

use super::bencode::Bencode;

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("invalid message")]
    InvalidMessage,

    #[error("bencode: {0}")]
    Bencode(#[from] super::bencode::Error),
}

#[derive(Clone)]
pub enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(usize),
    Bitfield(Vec<u8>),
    Request {
        piece_index: usize,
        piece_offset: usize,
        piece_len: usize,
    },
    Piece {
        piece_index: usize,
        piece_offset: usize,
        data: Vec<u8>,
    },
    Cancel {
        piece_index: usize,
        piece_offset: usize,
        piece_len: usize,
    },
    ExtensionHandshake(ExtensionHandshake),
    Extension {
        ext_id: u8,
        payload: Vec<u8>,
    },
    Unimplemented,
}

// BEP 10
#[derive(Clone)]
pub struct ExtensionHandshake {
    pub extensions: HashMap<String, u8>,
    pub metadata_size: Option<usize>, // BEP 9
    pub listen_port: Option<u16>,
    pub client: Option<String>,
    pub your_ip: Option<Vec<u8>>,
    pub ipv4: Option<[u8; 4]>,
    pub ipv6: Option<[u8; 16]>,
    pub reqq: Option<u32>,
}

impl Message {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            Message::KeepAlive => buf.extend_from_slice(&0u32.to_be_bytes()),
            Message::Choke => buf.extend_from_slice(&[0, 0, 0, 1, 0]),
            Message::Unchoke => buf.extend_from_slice(&[0, 0, 0, 1, 1]),
            Message::Interested => buf.extend_from_slice(&[0, 0, 0, 1, 2]),
            Message::NotInterested => buf.extend_from_slice(&[0, 0, 0, 1, 3]),
            Message::Have(piece) => {
                buf.extend_from_slice(&5u32.to_be_bytes());
                buf.push(4);
                buf.extend_from_slice(&(*piece as u32).to_be_bytes());
            },
            Message::Bitfield(bits) => {
                buf.extend_from_slice(&(1 + bits.len() as u32).to_be_bytes());
                buf.push(5);
                buf.extend_from_slice(bits);
            },
            Message::Request {
                piece_index,
                piece_offset,
                piece_len,
            } => {
                buf.extend_from_slice(&13u32.to_be_bytes());
                buf.push(6);
                buf.extend_from_slice(&(*piece_index as u32).to_be_bytes());
                buf.extend_from_slice(&(*piece_offset as u32).to_be_bytes());
                buf.extend_from_slice(&(*piece_len as u32).to_be_bytes());
            },
            Message::Piece {
                piece_index,
                piece_offset,
                data,
            } => {
                buf.extend_from_slice(&(9 + data.len() as u32).to_be_bytes());
                buf.push(7);
                buf.extend_from_slice(&(*piece_index as u32).to_be_bytes());
                buf.extend_from_slice(&(*piece_offset as u32).to_be_bytes());
                buf.extend_from_slice(data);
            },
            Message::Cancel {
                piece_index,
                piece_offset,
                piece_len,
            } => {
                buf.extend_from_slice(&13u32.to_be_bytes());
                buf.push(8);
                buf.extend_from_slice(&(*piece_index as u32).to_be_bytes());
                buf.extend_from_slice(&(*piece_offset as u32).to_be_bytes());
                buf.extend_from_slice(&(*piece_len as u32).to_be_bytes());
            },
            Message::ExtensionHandshake(hs) => {
                let payload = hs.encode();
                // [len][0x14][0x00][bencoded]
                buf.extend_from_slice(&(2 + payload.len() as u32).to_be_bytes());
                buf.push(20);
                buf.push(0);
                buf.extend_from_slice(&payload);
            },
            Message::Extension { ext_id, payload } => {
                // [len][0x14][ext_id][payload]
                buf.extend_from_slice(&(2 + payload.len() as u32).to_be_bytes());
                buf.push(20);
                buf.push(*ext_id);
                buf.extend_from_slice(payload);
            },
            Message::Unimplemented => {},
        }
        buf
    }

    pub fn decode(data: &[u8]) -> Result<Self, DecodeError> {
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
                    return Err(DecodeError::InvalidMessage);
                }
                Ok(Message::Have(u32::from_be_bytes(
                    payload
                        .try_into()
                        .map_err(|_| DecodeError::InvalidMessage)?,
                ) as usize))
            },
            5 => Ok(Message::Bitfield(payload.to_vec())),
            6 => {
                if payload.len() != 12 {
                    return Err(DecodeError::InvalidMessage);
                }
                Ok(Message::Request {
                    piece_index: u32::from_be_bytes(
                        payload[0..4]
                            .try_into()
                            .map_err(|_| DecodeError::InvalidMessage)?,
                    ) as usize,
                    piece_offset: u32::from_be_bytes(
                        payload[4..8]
                            .try_into()
                            .map_err(|_| DecodeError::InvalidMessage)?,
                    ) as usize,
                    piece_len: u32::from_be_bytes(
                        payload[8..12]
                            .try_into()
                            .map_err(|_| DecodeError::InvalidMessage)?,
                    ) as usize,
                })
            },
            7 => {
                if payload.len() < 8 {
                    return Err(DecodeError::InvalidMessage);
                }
                Ok(Message::Piece {
                    piece_index: u32::from_be_bytes(
                        payload[0..4]
                            .try_into()
                            .map_err(|_| DecodeError::InvalidMessage)?,
                    ) as usize,
                    piece_offset: u32::from_be_bytes(
                        payload[4..8]
                            .try_into()
                            .map_err(|_| DecodeError::InvalidMessage)?,
                    ) as usize,
                    data: payload[8..].to_vec(),
                })
            },
            8 => {
                if payload.len() != 12 {
                    return Err(DecodeError::InvalidMessage);
                }
                Ok(Message::Cancel {
                    piece_index: u32::from_be_bytes(
                        payload[0..4]
                            .try_into()
                            .map_err(|_| DecodeError::InvalidMessage)?,
                    ) as usize,
                    piece_offset: u32::from_be_bytes(
                        payload[4..8]
                            .try_into()
                            .map_err(|_| DecodeError::InvalidMessage)?,
                    ) as usize,
                    piece_len: u32::from_be_bytes(
                        payload[8..12]
                            .try_into()
                            .map_err(|_| DecodeError::InvalidMessage)?,
                    ) as usize,
                })
            },
            20 => {
                if payload.is_empty() {
                    return Err(DecodeError::InvalidMessage);
                }
                let ext_id = payload[0];
                let ext_payload = &payload[1..];
                if ext_id == 0 {
                    let bencoded = Bencode::decode(ext_payload)?;
                    return Ok(Message::ExtensionHandshake(
                        ExtensionHandshake::from_bencode(&bencoded)?,
                    ));
                }
                Ok(Message::Extension {
                    ext_id,
                    payload: ext_payload.to_vec(),
                })
            },
            _ => Ok(Message::Unimplemented),
        }
    }
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Piece {
                piece_index,
                piece_offset,
                data,
            } => f
                .debug_struct("Piece")
                .field("piece_index", piece_index)
                .field("piece_offset", piece_offset)
                .field("data", &data.len())
                .finish(),
            Self::Bitfield(bits) => write!(f, "Bitfield({} bytes)", bits.len()),
            Self::KeepAlive => write!(f, "KeepAlive"),
            Self::Choke => write!(f, "Choke"),
            Self::Unchoke => write!(f, "Unchoke"),
            Self::Interested => write!(f, "Interested"),
            Self::NotInterested => write!(f, "NotInterested"),
            Self::Have(piece) => write!(f, "Have({piece})"),
            Self::Request {
                piece_index,
                piece_offset,
                piece_len,
            } => f
                .debug_struct("Request")
                .field("piece_index", piece_index)
                .field("piece_offset", piece_offset)
                .field("piece_len", piece_len)
                .finish(),
            Self::Cancel {
                piece_index,
                piece_offset,
                piece_len,
            } => f
                .debug_struct("Cancel")
                .field("piece_index", piece_index)
                .field("piece_offset", piece_offset)
                .field("piece_len", piece_len)
                .finish(),
            Self::ExtensionHandshake(_) => f.debug_struct("ExtensionHandshake").finish(),
            Self::Extension { ext_id, .. } => {
                f.debug_struct("Extension").field("ext_id", ext_id).finish()
            },
            Self::Unimplemented => f.debug_struct("Unimplemented").finish(),
        }
    }
}

impl ExtensionHandshake {
    fn from_bencode(payload: &Bencode<'_>) -> Result<Self, DecodeError> {
        let extensions = match payload.get(b"m") {
            Ok(Bencode::Dict(m)) => m
                .iter()
                .filter_map(|(k, v)| {
                    let name = std::str::from_utf8(k).ok()?;
                    let id = match v {
                        Bencode::Int(n) if *n >= 0 && *n <= 255 => *n as u8,
                        _ => return None,
                    };
                    Some((name.to_owned(), id))
                })
                .collect(),
            _ => HashMap::new(),
        };
        let listen_port = payload
            .get_int(b"p")
            .ok()
            .and_then(|v| u16::try_from(v).ok());
        let your_ip = payload.get_bytes(b"yourip").ok().map(|b| b.to_vec());
        let client = payload.get_utf8(b"v").ok();
        let ipv4 = payload
            .get_bytes(b"ipv4")
            .ok()
            .and_then(|b| b.try_into().ok());
        let ipv6 = payload
            .get_bytes(b"ipv6")
            .ok()
            .and_then(|b| b.try_into().ok());
        let reqq = payload
            .get_int(b"reqq")
            .ok()
            .and_then(|v| u32::try_from(v).ok());

        // BEP 9
        let metadata_size = payload
            .get_int(b"metadata_size")
            .ok()
            .and_then(|v| usize::try_from(v).ok());

        Ok(Self {
            extensions,
            listen_port,
            client,
            your_ip,
            ipv4,
            ipv6,
            reqq,
            metadata_size,
        })
    }

    fn encode(&self) -> Vec<u8> {
        use std::collections::BTreeMap;

        let mut m: BTreeMap<&[u8], Bencode<'_>> = BTreeMap::new();
        for (name, &id) in &self.extensions {
            m.insert(name.as_bytes(), Bencode::Int(id as i64));
        }

        let mut dict: BTreeMap<&[u8], Bencode<'_>> = BTreeMap::new();
        dict.insert(b"m", Bencode::Dict(m));
        if let Some(port) = self.listen_port {
            dict.insert(b"p", Bencode::Int(port as i64));
        }
        if let Some(ref v) = self.client {
            dict.insert(b"v", Bencode::Bytes(v.as_bytes()));
        }
        if let Some(ref your_ip) = self.your_ip {
            dict.insert(b"yourip", Bencode::Bytes(your_ip));
        }
        if let Some(ref ipv4) = self.ipv4 {
            dict.insert(b"ipv4", Bencode::Bytes(ipv4));
        }
        if let Some(ref ipv6) = self.ipv6 {
            dict.insert(b"ipv6", Bencode::Bytes(ipv6));
        }
        if let Some(reqq) = self.reqq {
            dict.insert(b"reqq", Bencode::Int(reqq as i64));
        }
        if let Some(metadata_size) = self.metadata_size {
            dict.insert(b"metadata_size", Bencode::Int(metadata_size as i64));
        }

        Bencode::Dict(dict).encode()
    }
}
