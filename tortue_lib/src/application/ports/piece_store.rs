pub trait PieceStore: Send {
    fn write(&mut self, offset: u64, data: &[u8]) -> std::io::Result<()>;
}
