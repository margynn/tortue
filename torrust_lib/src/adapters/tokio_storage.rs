struct OutputFile {
    file: File,
    length: u64,
    offset: u64,
}

pub struct TokioStorage {
    files: Vec<OutputFile>,
}

impl TokioStorage {
    pub async fn new(metainfo: &Metainfo, root: PathBuf) -> Result<Self> {
        let files = Self::create_files(metainfo, root).await?;

        Ok(Self { files })
    }

    async fn create_files(
        metainfo: &Metainfo,
        root: PathBuf,
    ) -> Result<Vec<OutputFile>> {
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

                files.push(OutputFile { file, length: *length, offset });
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

                    files.push(OutputFile { file, length: f.length, offset });

                    offset += f.length;
                }
            },
        }

        Ok(files)
    }

    async fn write(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        let write_end = offset + data.len() as u64;

        for file in &mut self.files {
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
}

impl Storage for TokioStorage {
    type Error = std::io::Error;

    async fn execute(
        &mut self,
        command: StorageCommand,
    ) -> Result<(), Self::Error> {
        match command {
            StorageCommand::Write { offset, data } => {
                self.write(offset, &data).await
            },
        }
    }
}
