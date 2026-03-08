pub mod deserializer;

use std::sync::Arc;

pub trait MessageTypeValues {}

pub trait MessageType {
    fn value(&self) -> Box<dyn MessageTypeValues>;
}

pub enum DefaultMessageTypes {
    Default,
}

pub trait Message {
    fn content(&self) -> Arc<Vec<u8>>;
    fn get_type(&self) -> Arc<dyn MessageType + Send + Sync>;
    fn serialize(&self) -> Result<Vec<u8>, ()>;
}
