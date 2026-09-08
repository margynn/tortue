pub trait PieceStore: Send {
    // TODO: make sync, implementer can be async
    async fn write(&mut self, offset: u64, data: &[u8]) -> std::io::Result<()>;
}
