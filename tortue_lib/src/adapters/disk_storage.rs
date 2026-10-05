use std::{
    future::Future,
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
    domain::{
        block::BlockRange,
        torrent::{Metainfo, Mode},
    },
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
    cmd_tx: mpsc::Sender<DiskCommand>,
    piece_length: usize,
    piece_count: usize,
    total_size: u64,
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
    Flush {
        reply: oneshot::Sender<std::io::Result<()>>,
    },
}

impl PieceStore for DiskStorage {
    async fn write(&mut self, range: BlockRange, data: Vec<u8>) -> std::io::Result<()> {
        if data.len() != range.len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "write length does not match block range",
            ));
        }
        let offset = self.storage_offset(range)?;
        self.cmd_tx
            .send(DiskCommand::Write { offset, data })
            .await
            .map_err(|_| std::io::Error::other("writer task closed"))
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(DiskCommand::Flush { reply })
            .await
            .map_err(|_| std::io::Error::other("storage task closed"))?;
        rx.await
            .map_err(|_| std::io::Error::other("storage reply dropped"))?
    }

    fn read(
        &mut self,
        range: BlockRange,
    ) -> impl Future<Output = std::io::Result<Vec<u8>>> + Send + 'static {
        // Own the sender so the future does not borrow this storage handle.
        let tx = self.cmd_tx.clone();
        let offset = self.storage_offset(range);

        async move {
            let offset = offset?;
            let (reply, rx) = oneshot::channel();
            tx.send(DiskCommand::Read {
                offset,
                len: range.len,
                reply,
            })
            .await
            .map_err(|_| std::io::Error::other("reader task closed"))?;

            rx.await
                .map_err(|_| std::io::Error::other("storage reply dropped"))?
        }
    }
}

impl DiskStorage {
    // Bounds queued commands, not bytes: each write owns one piece buffer.
    const COMMAND_CAPACITY: usize = 8;

    pub async fn new(metainfo: &Metainfo, root: PathBuf) -> Result<Self> {
        let (cmd_tx, cmd_rx) = mpsc::channel(Self::COMMAND_CAPACITY);
        let files = Self::create_files(metainfo, root).await?;
        tokio::spawn(Self::worker(files, cmd_rx));
        Ok(Self {
            cmd_tx,
            piece_length: metainfo.piece_length,
            piece_count: metainfo.pieces.len(),
            total_size: metainfo.total_size(),
        })
    }

    fn storage_offset(&self, range: BlockRange) -> std::io::Result<u64> {
        let invalid_range = || {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "block range outside torrent or overflowing",
            )
        };
        let piece_end = range
            .piece_offset
            .checked_add(range.len)
            .ok_or_else(invalid_range)?;
        if range.piece_index >= self.piece_count || range.len == 0 || piece_end > self.piece_length
        {
            return Err(invalid_range());
        }
        let offset = (range.piece_index as u64)
            .checked_mul(self.piece_length as u64)
            .and_then(|start| start.checked_add(range.piece_offset as u64))
            .ok_or_else(invalid_range)?;
        let end = offset
            .checked_add(range.len as u64)
            .ok_or_else(invalid_range)?;
        if end > self.total_size {
            return Err(invalid_range());
        }
        Ok(offset)
    }

    async fn worker(mut files: Vec<OutputFile>, mut rx: mpsc::Receiver<DiskCommand>) {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                DiskCommand::Write { offset, data } => {
                    if let Err(error) = Self::write_to_files(&mut files, offset, &data).await {
                        tracing::error!(%error, "disk write failed");
                        return;
                    }
                },
                DiskCommand::Read { offset, len, reply } => {
                    if reply.is_closed() {
                        continue;
                    }
                    let result = Self::read_from_files(&mut files, offset, len).await;
                    let _ = reply.send(result);
                },
                DiskCommand::Flush { reply } => {
                    let result = Self::flush_files(&mut files).await;
                    let failed = result.is_err();
                    let _ = reply.send(result);
                    if failed {
                        return;
                    }
                },
            }
        }
    }

    async fn flush_files(files: &mut [OutputFile]) -> std::io::Result<()> {
        for file in files {
            // Wait for Tokio's blocking file IO, not crash-durable persistence.
            file.file.flush().await?;
        }
        Ok(())
    }

    async fn write_to_files(
        files: &mut Vec<OutputFile>,
        offset: u64,
        data: &[u8],
    ) -> std::io::Result<()> {
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

    async fn write_at(file: &mut File, offset: u64, data: &[u8]) -> std::io::Result<()> {
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
        // Prevent writing on symlink completly
        let metadata = std::fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            return Err(Error::PathTraversal);
        }
        Ok(root_path.join(path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_path_joins_relative_paths() {
        let root = Path::new("downloads");
        for (path, expected) in [
            ("file.bin", "downloads/file.bin"),
            ("dir/file.bin", "downloads/dir/file.bin"),
            ("dir/./file.bin", "downloads/dir/file.bin"),
            ("..hidden/file.bin", "downloads/..hidden/file.bin"),
            ("été/fichier.bin", "downloads/été/fichier.bin"),
            ("", "downloads"),
            (".", "downloads"),
        ] {
            assert_eq!(
                DiskStorage::build_path(root, Path::new(path)).unwrap(),
                PathBuf::from(expected),
                "path: {path}"
            );
        }
    }

    #[test]
    fn build_path_rejects_parent_directory_components() {
        for path in [
            "..",
            "../file.bin",
            "dir/../file.bin",
            "dir/../../file.bin",
            "./../file.bin",
        ] {
            assert!(
                matches!(
                    DiskStorage::build_path(Path::new("downloads"), Path::new(path)),
                    Err(Error::PathTraversal)
                ),
                "path: {path}"
            );
        }
    }

    #[test]
    fn build_path_rejects_absolute_paths() {
        #[cfg(not(windows))]
        let paths = ["/file.bin", "/tmp/dir/file.bin"];
        #[cfg(windows)]
        let paths = [
            r"C:\file.bin",
            r"C:\dir\file.bin",
            r"\\server\share\file.bin",
            r"\\?\C:\file.bin",
        ];

        for path in paths {
            assert!(
                matches!(
                    DiskStorage::build_path(Path::new("downloads"), Path::new(path)),
                    Err(Error::PathTraversal)
                ),
                "path: {path}"
            );
        }
    }
}
