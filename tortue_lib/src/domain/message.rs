use std::{collections::HashMap, fmt};

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
    Unimplemented, // special case for unknown messages
}

// BEP 10
#[derive(Clone)]
pub struct ExtensionHandshake {
    pub extensions: HashMap<String, u8>,
    pub listen_port: Option<u16>,
    pub client: Option<String>,
    pub your_ip: Option<Vec<u8>>,
    pub ipv4: Option<[u8; 4]>,
    pub ipv6: Option<[u8; 16]>,
    pub reqq: Option<u32>,
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
            Self::ExtensionHandshake(_) => f.debug_struct("extension_handshake").finish(),
            Self::Extension { .. } => f.debug_struct("extension_message").finish(),
            Self::Unimplemented => f.debug_struct("Unimplemented").finish(),
        }
    }
}
