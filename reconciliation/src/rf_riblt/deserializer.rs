use std::sync::Arc;

use connection::request::handler::default::{TestMessage, TestMessageType};
use message::Message;
use protocol::deserializer::ProtocolDeserializer;
use rkyv::{from_bytes, rancor::Error};
use tracing::error;

use crate::rf_riblt::messages::RfRibltMessageWrapper;

#[derive(Default)]
pub struct RfRibltDeserializer {}

impl RfRibltDeserializer {
    pub fn new() -> Self {
        Self {}
    }
}

impl ProtocolDeserializer for RfRibltDeserializer {
    fn deserialize(&self, bytes: Vec<u8>) -> Arc<dyn Message + Send + Sync> {
        if bytes.len() < 16 {
            return Arc::new(TestMessage::new(Arc::new(TestMessageType::new()), None));
        }

        let payload = &bytes[16..];

        match from_bytes::<RfRibltMessageWrapper, Error>(payload) {
            Ok(wrapper) => match wrapper {
                RfRibltMessageWrapper::Handshake(msg) => Arc::new(msg),
                RfRibltMessageWrapper::FilterChunk(msg) => Arc::new(msg),
                RfRibltMessageWrapper::FilterDone(msg) => Arc::new(msg),
                RfRibltMessageWrapper::SComSendSymbol(msg) => Arc::new(msg),
                RfRibltMessageWrapper::SComDecodedAll(msg) => Arc::new(msg),
                RfRibltMessageWrapper::SComRequestMoreSymbols(msg) => Arc::new(msg),
                RfRibltMessageWrapper::ValueFetchRequest(msg) => Arc::new(msg),
                RfRibltMessageWrapper::ValueFetchResponse(msg) => Arc::new(msg),
            },
            Err(e) => {
                error!("Failed to deserialize RF-RIBLT message: {}", e);
                Arc::new(TestMessage::new(Arc::new(TestMessageType::new()), None))
            }
        }
    }
}
