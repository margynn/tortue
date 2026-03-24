pub mod decode;
const SHA_LENGTH: usize = 20;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("bencode parsing failed: {0}")]
    Bencode(#[from] crate::bencode::Error),
    #[error("invalid UTF-8 in announce")]
    InvalidUtf8Announce,
    #[error("invalid UTF-8 in name")]
    InvalidUtf8Name,
    #[error("invalid dictionary key")]
    InvalidDictKey,
}

#[derive(Debug, Clone)]
pub struct Metainfo {
    announce: Vec<u8>,
    info: InfoDictionary,
}

#[derive(Debug, Clone)]
pub struct InfoDictionary {
    name: Vec<u8>,
    piece_length: usize,
    pieces: Vec<[u8; SHA_LENGTH]>,
    mode: Mode,
}

#[derive(Debug, Clone)]
pub enum Mode {
    Single { length: usize },
    Multiple { files: Vec<File> },
}

#[derive(Debug, Clone)]
pub struct File {
    length: usize,
    path: Vec<Vec<u8>>,
}

impl Metainfo {
    fn announce_str(&self) -> Option<&str> {
        std::str::from_utf8(self.announce.as_ref()).ok()
    }
}

impl InfoDictionary {
    fn name_str(&self) -> Option<&str> {
        std::str::from_utf8(self.name.as_ref()).ok()
    }
}

impl File {
    fn path_strs(&self) -> Vec<&str> {
        self.path.iter().filter_map(|p| std::str::from_utf8(p).ok()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metainfo_announce_str_valid() {
        let metainfo = Metainfo {
            announce: b"http://tracker.example.com".to_vec(),
            info: InfoDictionary {
                name: b"test".to_vec(),
                piece_length: 16384,
                pieces: vec![],
                mode: Mode::Single { length: 100 },
            },
        };
        assert_eq!(metainfo.announce_str(), Some("http://tracker.example.com"));
    }

    #[test]
    fn test_metainfo_announce_str_invalid_utf8() {
        let metainfo = Metainfo {
            announce: vec![0xFF, 0xFE],
            info: InfoDictionary {
                name: b"test".to_vec(),
                piece_length: 16384,
                pieces: vec![],
                mode: Mode::Single { length: 100 },
            },
        };
        assert_eq!(metainfo.announce_str(), None);
    }

    #[test]
    fn test_info_dictionary_name_str_valid() {
        let info = InfoDictionary {
            name: b"test_file".to_vec(),
            piece_length: 16384,
            pieces: vec![],
            mode: Mode::Single { length: 100 },
        };
        assert_eq!(info.name_str(), Some("test_file"));
    }

    #[test]
    fn test_info_dictionary_name_str_invalid_utf8() {
        let info = InfoDictionary {
            name: vec![0xFF, 0xFE],
            piece_length: 16384,
            pieces: vec![],
            mode: Mode::Single { length: 100 },
        };
        assert_eq!(info.name_str(), None);
    }

    #[test]
    fn test_file_path_strs_valid() {
        let file = File {
            length: 100,
            path: vec![b"dir1".to_vec(), b"dir2".to_vec(), b"file.txt".to_vec()],
        };
        let path_strs = file.path_strs();
        assert_eq!(path_strs, vec!["dir1", "dir2", "file.txt"]);
    }

    #[test]
    fn test_file_path_strs_with_invalid_utf8() {
        let file = File {
            length: 100,
            path: vec![b"dir1".to_vec(), vec![0xFF, 0xFE], b"file.txt".to_vec()],
        };
        let path_strs = file.path_strs();
        assert_eq!(path_strs, vec!["dir1", "file.txt"]);
    }

    #[test]
    fn test_file_path_strs_empty() {
        let file = File {
            length: 100,
            path: vec![],
        };
        let path_strs = file.path_strs();
        assert_eq!(path_strs, Vec::<&str>::new());
    }

    #[test]
    fn test_mode_single() {
        let mode = Mode::Single { length: 12345 };
        match mode {
            Mode::Single { length } => assert_eq!(length, 12345),
            Mode::Multiple { .. } => panic!("expected single mode"),
        }
    }

    #[test]
    fn test_mode_multiple() {
        let files = vec![
            File { length: 100, path: vec![b"file1".to_vec()] },
            File { length: 200, path: vec![b"dir".to_vec(), b"file2".to_vec()] },
        ];
        let mode = Mode::Multiple { files: files.clone() };
        match mode {
            Mode::Multiple { files: f } => {
                assert_eq!(f.len(), 2);
                assert_eq!(f[0].length, 100);
                assert_eq!(f[1].length, 200);
            }
            Mode::Single { .. } => panic!("expected multiple mode"),
        }
    }
}
