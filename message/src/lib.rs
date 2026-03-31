pub mod deserializer;

use std::any::Any;
use std::sync::Arc;

pub trait MessageTypeValues: Any {
    fn as_any(&self) -> &dyn Any;
}

pub trait MessageType {
    fn value(&self) -> Box<dyn MessageTypeValues>;
}

pub enum DefaultMessageTypes {
    Default,
}

pub trait Message: Any + Send + Sync {
    fn content(&self) -> Arc<Vec<u8>>;
    fn get_type(&self) -> Arc<dyn MessageType + Send + Sync>;
    fn serialize(&self, protocol: Option<u64>, sender_port: u16) -> Result<Vec<u8>, ()>;
    fn protocol(&self) -> Option<u64>;
    fn as_any(&self) -> &dyn Any;
}
