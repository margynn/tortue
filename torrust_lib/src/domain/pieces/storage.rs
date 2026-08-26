#[derive(Debug)]
pub enum StorageCommand {
    Write { offset: u64, data: Vec<u8> },
}

#[derive(Debug)]
pub struct StorageError {
    // implementation à faire
}

pub trait Storage {
    type Error;

    async fn execute(
        &mut self,
        command: StorageCommand,
    ) -> Result<(), Self::Error>;
}
