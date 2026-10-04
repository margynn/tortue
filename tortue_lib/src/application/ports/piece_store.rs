use crate::domain::block::BlockRange;

pub trait PieceStore: Send {
    async fn write(&mut self, range: BlockRange, data: Vec<u8>) -> std::io::Result<()>;
    async fn flush(&mut self) -> std::io::Result<()>;
    async fn read(&mut self, range: BlockRange) -> std::io::Result<Vec<u8>>;
}
