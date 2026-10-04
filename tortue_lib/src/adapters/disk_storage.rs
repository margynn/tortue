use std::{
    io::SeekFrom,
    path::{Component, Path, PathBuf},
};

use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{mpsc, oneshot},
};

use crate::{
    application::ports::piece_store::PieceStore,
    domain::torrent::{Metainfo, Mode},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("path traversal")]
    PathTraversal,

    #[error("invalid file")]
    InvalidFile,
}
type Result<T> = std::result::Result<T, Error>;

struct OutputFile {
    file: File,
    length: u64,
    offset: u64,
}

pub struct DiskStorage {
    cmd_tx: mpsc::UnboundedSender<DiskCommand>,
}

enum DiskCommand {
    Write {
        offset: u64,
        data: Vec<u8>,
    },
    Read {
        offset: u64,
        len: usize,
        reply: oneshot::Sender<std::io::Result<Vec<u8>>>,
    },
}

impl PieceStore for DiskStorage {
    fn write(&mut self, offset: u64, data: Vec<u8>) -> std::io::Result<()> {
        self.cmd_tx
            .send(DiskCommand::Write { offset, data })
            .map_err(|_| std::io::Error::other("writer task closed"))
    }

    async fn read(&mut self, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.cmd_tx
            .send(DiskCommand::Read {
                offset,
                len,
                reply: reply_tx,
            })
            .map_err(|_| std::io::Error::other("reader task closed"))?;

        reply_rx
            .await
            .map_err(|_| std::io::Error::other("storage reply dropped"))?
    }
}

impl DiskStorage {
    pub async fn new(metainfo: &Metainfo, root: PathBuf) -> Result<Self> {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let files = Self::create_files(metainfo, root).await?;
        tokio::spawn(Self::worker(files, cmd_rx));
        Ok(Self { cmd_tx })
    }

    async fn worker(mut files: Vec<OutputFile>, mut rx: mpsc::UnboundedReceiver<DiskCommand>) {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                DiskCommand::Write { offset, data } => {
                    if let Err(e) = Self::write_to_files(&mut files, offset, &data).await {
                        tracing::error!(error = %e, "disk write failed");
                    }
                },
                DiskCommand::Read { offset, len, reply } => {
                    let result = Self::read_from_files(&mut files, offset, len).await;
                    let _ = reply.send(result);
                },
            }
        }
    }

    async fn write_to_files(files: &mut Vec<OutputFile>, offset: u64, data: &[u8]) -> Result<()> {
        let write_end = offset + data.len() as u64;
        for file in files.iter_mut() {
            let file_start = file.offset;
            let file_end = file.offset + file.length;
            if offset >= file_end || write_end <= file_start {
                continue;
            }
            let write_start = offset.max(file_start);
            let write_end = write_end.min(file_end);
            let buffer_start = (write_start - offset) as usize;
            let len = (write_end - write_start) as usize;
            Self::write_at(
                &mut file.file,
                write_start - file_start,
                &data[buffer_start..buffer_start + len],
            )
            .await?;
        }
        Ok(())
    }

    async fn read_from_files(
        files: &mut [OutputFile],
        offset: u64,
        len: usize,
    ) -> std::io::Result<Vec<u8>> {
        let read_end = offset
            .checked_add(len as u64)
            .ok_or_else(|| std::io::Error::other("read range overflow"))?;
        let mut buffer = Vec::with_capacity(len);
        for file in files {
            let file_end = file
                .offset
                .checked_add(file.length)
                .ok_or_else(|| std::io::Error::other("file range overflow"))?;
            let start = offset.max(file.offset);
            let end = read_end.min(file_end);
            if start >= end {
                continue;
            }
            let expected = (end - start) as usize;
            let read =
                Self::read_from(&mut file.file, start - file.offset, expected, &mut buffer).await?;
            if read != expected {
                break;
            }
        }
        Ok(buffer)
    }

    async fn write_at(file: &mut File, offset: u64, data: &[u8]) -> Result<()> {
        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(data).await?;
        Ok(())
    }

    async fn read_from(
        file: &mut File,
        offset: u64,
        len: usize,
        buffer: &mut Vec<u8>,
    ) -> std::io::Result<usize> {
        file.seek(SeekFrom::Start(offset)).await?;
        file.take(len as u64).read_to_end(buffer).await
    }

    /// Opens or creates the torrent files and records their offsets.
    /// Rejects absolute paths and parent-directory components (`..`).
    /// Creates missing parent directories and preserves existing file contents.
    /// Does not prevent symlink traversal or guarantee available disk space.
    async fn create_files(metainfo: &Metainfo, root: PathBuf) -> Result<Vec<OutputFile>> {
        let mut files = Vec::new();
        let mut offset = 0u64;
        let base = Self::build_path(&root, &Path::new(&metainfo.name))?;

        if let Some(parent) = base.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        match &metainfo.mode {
            Mode::Single { length } => {
                let file = Self::open_file(base, *length).await?;
                files.push(OutputFile {
                    file,
                    length: *length,
                    offset,
                });
            },

            Mode::Multiple { files: meta_files } => {
                for f in meta_files {
                    let file_path = Self::build_path(&base, &PathBuf::from_iter(&f.path))?;
                    if let Some(parent) = file_path.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    let file = Self::open_file(file_path, f.length).await?;
                    files.push(OutputFile {
                        file,
                        length: f.length,
                        offset,
                    });
                    offset += f.length;
                }
            },
        }

        Ok(files)
    }

    /// Creates a new file exclusively and sets its length, or opens an existing
    /// regular file of the expected length without truncating or resizing it.
    /// Existing-file checks use metadata from the opened handle.
    /// Symlinks are followed; matching length does not verify torrent contents.
    async fn open_file(file_path: PathBuf, length: u64) -> Result<File> {
        match OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&file_path)
            .await
        {
            Ok(file) => {
                // Only newly created files may be resized.
                file.set_len(length).await?;
                Ok(file)
            },
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&file_path)
                    .await?;
                let metadata = file.metadata().await?;
                if !metadata.is_file() {
                    return Err(Error::InvalidFile);
                }
                if metadata.len() > length {
                    return Err(Error::InvalidFile);
                }
                file.set_len(length).await?;
                Ok(file)
            },
            Err(error) => Err(error.into()),
        }
    }

    fn build_path(root_path: &Path, path: &Path) -> Result<PathBuf> {
        if path.is_absolute() || path.components().any(|c| c == Component::ParentDir) {
            return Err(Error::PathTraversal);
        }
        Ok(root_path.join(path))
    }
}
