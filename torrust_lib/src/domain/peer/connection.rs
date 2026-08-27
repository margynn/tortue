use super::{Message, Result};

trait Connection {
    async fn read_message(&mut self) -> Result<Message>;
    async fn send(&mut self, msg: Message) -> Result<()>;
}
