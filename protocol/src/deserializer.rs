use message::Message;
use std::sync::Arc;

pub trait ProtocolDeserializer {
    fn deserialize(&self, bytes: Vec<u8>) -> Arc<dyn Message>;
}
