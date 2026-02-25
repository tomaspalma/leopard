use std::io::Bytes;

use message::{Message, MessageType};

pub mod default;

pub trait RequestHandler<SType> {
    fn handle(&self, stream: Bytes<SType>) -> Box<dyn Message + Send + Sync>;
}
