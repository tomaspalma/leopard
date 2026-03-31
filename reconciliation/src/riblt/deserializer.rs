use connection::request::handler::default::{TestMessage, TestMessageType};
use message::Message;
use protocol::deserializer::ProtocolDeserializer;
use rkyv::{from_bytes, rancor::Error};
use std::sync::Arc;
use tracing::error;

use crate::riblt::messages::RIBLTMessageWrapper;

#[derive(Default)]
pub struct RIBLTDeserializer {}

impl RIBLTDeserializer {
    pub fn new() -> Self {
        Self {}
    }
}

impl ProtocolDeserializer for RIBLTDeserializer {
    fn deserialize(&self, bytes: Vec<u8>) -> Arc<dyn Message> {
        if bytes.len() < 16 {
            return Arc::new(TestMessage::new(Arc::new(TestMessageType::new()), None));
        }

        let payload = &bytes[16..];

        match from_bytes::<RIBLTMessageWrapper, Error>(payload) {
            Ok(wrapper) => match wrapper {
                RIBLTMessageWrapper::SendSymbol(msg) => Arc::new(msg),
                RIBLTMessageWrapper::DecodedAll(msg) => Arc::new(msg),
            },
            Err(e) => {
                error!("Failed to deserialize RIBLT message: {}", e);
                Arc::new(TestMessage::new(Arc::new(TestMessageType::new()), None))
            }
        }
    }
}
