pub trait PieceStore: Send {
    async fn write(&mut self, offset: u64, data: Vec<u8>) -> std::io::Result<()>;
    async fn flush(&mut self) -> std::io::Result<()>;
    async fn read(&mut self, offset: u64, len: usize) -> std::io::Result<Vec<u8>>;
}
