use std::io::Bytes;

use message::{DefaultMessageType, Message, MessageType};

pub mod default;

pub trait RequestHandler<M, MType, SType>
where
    M: Message<MType>,
    MType: MessageType,
{
    fn handle(&self, stream: Bytes<SType>) -> Box<dyn Message<DefaultMessageType>>;
}
