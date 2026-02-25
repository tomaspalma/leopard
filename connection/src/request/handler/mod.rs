use std::{io::Bytes, sync::Arc};

use message::{Message, MessageType};

pub mod default;

pub trait RequestHandler<SType> {
    fn handle(&self, stream: Bytes<SType>) -> Arc<dyn Message + Send + Sync>;
}
