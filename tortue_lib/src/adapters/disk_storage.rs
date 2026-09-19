use std::path::PathBuf;

use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::mpsc,
};

use crate::{
    application::ports::piece_store::PieceStore,
    domain::torrent::{Metainfo, Mode},
};

type Result<T> = std::io::Result<T>;

struct OutputFile {
    file: File,
    length: u64,
    offset: u64,
}

pub struct DiskStorage {
    buffer_tx: mpsc::UnboundedSender<(u64, Vec<u8>)>,
}

impl PieceStore for DiskStorage {
    fn write(&mut self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        self.buffer_tx
            .send((offset, data.to_vec()))
            .map_err(|_| std::io::Error::other("writer task closed"))
    }
}

impl DiskStorage {
    pub async fn new(metainfo: &Metainfo, root: PathBuf) -> Result<Self> {
        let (buffer_tx, buffer_rx) = mpsc::unbounded_channel();
        let files = Self::create_files(metainfo, root).await?;
        tokio::spawn(Self::writer_task(files, buffer_rx));
        Ok(Self { buffer_tx })
    }

    async fn writer_task(
        mut files: Vec<OutputFile>,
        mut rx: mpsc::UnboundedReceiver<(u64, Vec<u8>)>,
    ) {
        while let Some((offset, data)) = rx.recv().await {
            if let Err(e) = Self::write_to_files(&mut files, offset, &data).await {
                tracing::error!(error = %e, "disk write failed");
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
            file.file
                .seek(std::io::SeekFrom::Start(write_start - file_start))
                .await?;
            file.file
                .write_all(&data[buffer_start..buffer_start + len])
                .await?;
        }
        Ok(())
    }

    async fn create_files(metainfo: &Metainfo, root: PathBuf) -> Result<Vec<OutputFile>> {
        let mut files = Vec::new();
        let mut offset = 0u64;
        let base = root.join(&metainfo.name);

        match &metainfo.mode {
            Mode::Single { length } => {
                if let Some(parent) = base.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                let file = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .read(true)
                    .write(true)
                    .open(&base)
                    .await?;
                file.set_len(*length).await?;
                files.push(OutputFile {
                    file,
                    length: *length,
                    offset,
                });
            },

            Mode::Multiple { files: meta_files } => {
                tokio::fs::create_dir_all(&base).await?;
                for f in meta_files {
                    let file_path = base.join(PathBuf::from_iter(&f.path));
                    if let Some(parent) = file_path.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    let file = OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .read(true)
                        .write(true)
                        .open(&file_path)
                        .await?;
                    file.set_len(f.length).await?;
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
}
