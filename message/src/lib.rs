pub mod deserializer;
pub mod macros;

use std::any::Any;
use std::sync::Arc;

pub struct ProtocolIDTranslator;

impl ProtocolIDTranslator {
    pub fn translate(protocol_id: u64) -> &'static str {
        match protocol_id {
            1 => "riblt",
            2 => "merkle",
            3 => "rbf_riblt",
            5 => "replication",
            _ => "other",
        }
    }
}

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

pub fn build_message_packet(
    protocol: Option<u64>,
    sender_port: u16,
    body_bytes: impl AsRef<[u8]>,
) -> Vec<u8> {
    let body_bytes = body_bytes.as_ref();
    let mut packet = Vec::with_capacity(body_bytes.len() + 16);

    if let Some(id) = protocol {
        packet.extend_from_slice(&id.to_be_bytes());
    } else {
        packet.extend_from_slice(&[0; 8]);
    }

    packet.extend_from_slice(&sender_port.to_be_bytes());
    packet.extend_from_slice(&[0; 6]);
    packet.extend_from_slice(&body_bytes);

    packet
}

