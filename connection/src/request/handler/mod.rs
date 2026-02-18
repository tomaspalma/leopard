use message::{Message, MessageType};

pub mod default;

pub trait RequestHandler<M, MType>
where
    M: Message<MType>,
    MType: MessageType,
{
    fn handle(&self);
}
