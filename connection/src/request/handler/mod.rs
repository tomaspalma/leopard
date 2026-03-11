use std::{io::Bytes, sync::Arc};

use message::{Message, MessageType};

pub mod default;

pub trait RequestHandler<SType, RType> {
    fn handle(&self, stream: SType) -> RType;
}
