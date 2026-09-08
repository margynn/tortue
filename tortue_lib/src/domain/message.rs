use std::{
    collections::{BTreeMap, HashMap},
    fmt,
};

use super::bencode::Bencode;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid message")]
    InvalidMessage,

    #[error("bencode: {0}")]
    Bencode(#[from] super::bencode::Error),
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
pub enum Message {
    // BEP 3 - Core
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

    // BEP 10 - Extension Protocol
    ExtensionHandshake(ExtensionHandshake),
    Extension {
        ext_id: u8,
        payload: Vec<u8>,
    },

    // BEP 6 - Fast Extension
    SuggestPiece(usize),
    HaveAll,
    HaveNone,
    RejectRequest {
        piece_index: usize,
        piece_offset: usize,
        piece_len: usize,
    },
    AllowedFast(usize),

    // Safety
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
    /// Encodes the message as [msg_id][data...].
    /// Does NOT include the 4-byte TCP length prefix — framing is the transport layer's
    /// responsibility. See `frame()` in peer_io, which is the symmetric counterpart of
    /// `Message::read_from` (strip length → decode vs encode → prepend length).
    pub fn encode(&self) -> Vec<u8> {
        match self {
            // KeepAlive has no msg_id — empty payload frames as [0,0,0,0]
            Message::KeepAlive => vec![],
            Message::Choke => vec![0],
            Message::Unchoke => vec![1],
            Message::Interested => vec![2],
            Message::NotInterested => vec![3],
            Message::Have(piece) => {
                let mut buf = vec![4];
                buf.extend_from_slice(&(*piece as u32).to_be_bytes());
                buf
            },
            Message::Bitfield(bits) => {
                let mut buf = vec![5];
                buf.extend_from_slice(bits);
                buf
            },
            Message::Request {
                piece_index,
                piece_offset,
                piece_len,
            } => {
                let mut buf = vec![6];
                buf.extend_from_slice(&(*piece_index as u32).to_be_bytes());
                buf.extend_from_slice(&(*piece_offset as u32).to_be_bytes());
                buf.extend_from_slice(&(*piece_len as u32).to_be_bytes());
                buf
            },
            Message::Piece {
                piece_index,
                piece_offset,
                data,
            } => {
                let mut buf = vec![7];
                buf.extend_from_slice(&(*piece_index as u32).to_be_bytes());
                buf.extend_from_slice(&(*piece_offset as u32).to_be_bytes());
                buf.extend_from_slice(data);
                buf
            },
            Message::Cancel {
                piece_index,
                piece_offset,
                piece_len,
            } => {
                let mut buf = vec![8];
                buf.extend_from_slice(&(*piece_index as u32).to_be_bytes());
                buf.extend_from_slice(&(*piece_offset as u32).to_be_bytes());
                buf.extend_from_slice(&(*piece_len as u32).to_be_bytes());
                buf
            },
            Message::ExtensionHandshake(hs) => {
                // ext_id 0 = handshake (BEP 10)
                let mut buf = vec![20, 0];
                buf.extend_from_slice(&hs.encode());
                buf
            },
            Message::Extension { ext_id, payload } => {
                let mut buf = vec![20, *ext_id];
                buf.extend_from_slice(payload);
                buf
            },
            Message::HaveAll => vec![14],
            Message::HaveNone => vec![15],
            Message::SuggestPiece(piece) => {
                let mut buf = vec![13];
                buf.extend_from_slice(&(*piece as u32).to_be_bytes());
                buf
            },
            Message::RejectRequest {
                piece_index,
                piece_offset,
                piece_len,
            } => {
                let mut buf = vec![16];
                buf.extend_from_slice(&(*piece_index as u32).to_be_bytes());
                buf.extend_from_slice(&(*piece_offset as u32).to_be_bytes());
                buf.extend_from_slice(&(*piece_len as u32).to_be_bytes());
                buf
            },
            Message::AllowedFast(piece) => {
                let mut buf = vec![17];
                buf.extend_from_slice(&(*piece as u32).to_be_bytes());
                buf
            },
            Message::Unimplemented => vec![],
        }
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
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
                Ok(Message::Have(Self::read_u32(payload, 0)?))
            },
            5 => Ok(Message::Bitfield(payload.to_vec())),
            6 => {
                if payload.len() != 12 {
                    return Err(Error::InvalidMessage);
                }
                Ok(Message::Request {
                    piece_index: Self::read_u32(payload, 0)?,
                    piece_offset: Self::read_u32(payload, 4)?,
                    piece_len: Self::read_u32(payload, 8)?,
                })
            },
            7 => {
                if payload.len() < 8 {
                    return Err(Error::InvalidMessage);
                }
                Ok(Message::Piece {
                    piece_index: Self::read_u32(payload, 0)?,
                    piece_offset: Self::read_u32(payload, 4)?,
                    data: payload[8..].to_vec(),
                })
            },
            8 => {
                if payload.len() != 12 {
                    return Err(Error::InvalidMessage);
                }
                Ok(Message::Cancel {
                    piece_index: Self::read_u32(payload, 0)?,
                    piece_offset: Self::read_u32(payload, 4)?,
                    piece_len: Self::read_u32(payload, 8)?,
                })
            },
            20 => {
                if payload.is_empty() {
                    return Err(Error::InvalidMessage);
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
            13 => {
                if payload.len() != 4 {
                    return Err(Error::InvalidMessage);
                }
                Ok(Message::SuggestPiece(Self::read_u32(payload, 0)?))
            },
            14 => {
                if !payload.is_empty() {
                    return Err(Error::InvalidMessage);
                }
                Ok(Message::HaveAll)
            },
            15 => {
                if !payload.is_empty() {
                    return Err(Error::InvalidMessage);
                }
                Ok(Message::HaveNone)
            },
            16 => {
                if payload.len() != 12 {
                    return Err(Error::InvalidMessage);
                }
                Ok(Message::RejectRequest {
                    piece_index: Self::read_u32(payload, 0)?,
                    piece_offset: Self::read_u32(payload, 4)?,
                    piece_len: Self::read_u32(payload, 8)?,
                })
            },
            17 => {
                if payload.len() != 4 {
                    return Err(Error::InvalidMessage);
                }
                Ok(Message::AllowedFast(Self::read_u32(payload, 0)?))
            },
            _ => Ok(Message::Unimplemented),
        }
    }

    fn read_u32(payload: &[u8], offset: usize) -> Result<usize> {
        Ok(u32::from_be_bytes(
            payload[offset..offset + 4]
                .try_into()
                .map_err(|_| Error::InvalidMessage)?,
        ) as usize)
    }
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Message::Piece {
                piece_index,
                piece_offset,
                data,
            } => f
                .debug_struct("Piece")
                .field("piece_index", piece_index)
                .field("piece_offset", piece_offset)
                .field("data", &data.len())
                .finish(),
            Message::Bitfield(bits) => write!(f, "Bitfield({} bytes)", bits.len()),
            Message::KeepAlive => write!(f, "KeepAlive"),
            Message::Choke => write!(f, "Choke"),
            Message::Unchoke => write!(f, "Unchoke"),
            Message::Interested => write!(f, "Interested"),
            Message::NotInterested => write!(f, "NotInterested"),
            Message::Have(piece) => write!(f, "Have({piece})"),
            Message::Request {
                piece_index,
                piece_offset,
                piece_len,
            } => f
                .debug_struct("Request")
                .field("piece_index", piece_index)
                .field("piece_offset", piece_offset)
                .field("piece_len", piece_len)
                .finish(),
            Message::Cancel {
                piece_index,
                piece_offset,
                piece_len,
            } => f
                .debug_struct("Cancel")
                .field("piece_index", piece_index)
                .field("piece_offset", piece_offset)
                .field("piece_len", piece_len)
                .finish(),
            Message::ExtensionHandshake(_) => f.debug_struct("ExtensionHandshake").finish(),
            Message::Extension { ext_id, .. } => {
                f.debug_struct("Extension").field("ext_id", ext_id).finish()
            },
            Message::SuggestPiece(piece) => write!(f, "SuggestPiece({piece})"),
            Message::HaveAll => f.debug_struct("HaveAll").finish(),
            Message::HaveNone => f.debug_struct("HaveNone").finish(),
            Message::RejectRequest {
                piece_index,
                piece_offset,
                piece_len,
            } => f
                .debug_struct("RejectRequest")
                .field("piece_index", piece_index)
                .field("piece_offset", piece_offset)
                .field("piece_len", piece_len)
                .finish(),
            Message::AllowedFast(piece) => write!(f, "AllowedFast({piece})"),
            Message::Unimplemented => f.debug_struct("Unimplemented").finish(),
        }
    }
}

impl ExtensionHandshake {
    fn from_bencode(payload: &Bencode<'_>) -> Result<Self> {
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

// BEP 9 - Listing our extension ids for Tortue client
pub const UT_METADATA_EXT_ID: u8 = 1;

pub enum UtMetadataMessage {
    Request {
        piece: usize,
    },
    Data {
        piece: usize,
        total_size: usize,
        data: Vec<u8>,
    },
    Reject {
        piece: usize,
    },
    Unimplemented,
}

impl UtMetadataMessage {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        let (dict, rest) = Bencode::decode_with_rest(payload)?;
        let msg_type = dict.get_int(b"msg_type")?;
        let piece = dict.get_int(b"piece")? as usize;
        match msg_type {
            0 => Ok(Self::Request { piece }),
            1 => {
                let total_size = dict.get_int(b"total_size")? as usize;
                Ok(Self::Data {
                    piece,
                    total_size,
                    data: rest.to_vec(),
                })
            },
            2 => Ok(Self::Reject { piece }),
            _ => Ok(Self::Unimplemented),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut dict: BTreeMap<&[u8], Bencode> = BTreeMap::new();
        match self {
            Self::Request { piece } => {
                dict.insert(b"msg_type", Bencode::Int(0));
                dict.insert(b"piece", Bencode::Int(*piece as i64));
                Bencode::Dict(dict).encode()
            },
            Self::Data {
                piece,
                total_size,
                data,
            } => {
                dict.insert(b"msg_type", Bencode::Int(1));
                dict.insert(b"piece", Bencode::Int(*piece as i64));
                dict.insert(b"total_size", Bencode::Int(*total_size as i64));
                let mut out = Bencode::Dict(dict).encode();
                out.extend_from_slice(data);
                out
            },
            Self::Reject { piece } => {
                dict.insert(b"msg_type", Bencode::Int(2));
                dict.insert(b"piece", Bencode::Int(*piece as i64));
                Bencode::Dict(dict).encode()
            },
            Self::Unimplemented => vec![],
        }
    }
}
