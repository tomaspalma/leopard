use std::sync::Arc;

use connection::request::handler::default::{TestMessage, TestMessageType};
use message::Message;
use protocol::deserializer::ProtocolDeserializer;
use rkyv::{from_bytes, rancor::Error};
use tracing::error;

use crate::rbf_riblt::messages::RbfRibltMessageWrapper;

#[derive(Default)]
pub struct RbfRibltDeserializer {}

impl RbfRibltDeserializer {
    pub fn new() -> Self {
        Self {}
    }
}

impl ProtocolDeserializer for RbfRibltDeserializer {
    fn deserialize(&self, bytes: Vec<u8>) -> Arc<dyn Message + Send + Sync> {
        if bytes.len() < 16 {
            return Arc::new(TestMessage::new(Arc::new(TestMessageType::new()), None));
        }

        let payload = &bytes[16..];

        match from_bytes::<RbfRibltMessageWrapper, Error>(payload) {
            Ok(wrapper) => match wrapper {
                RbfRibltMessageWrapper::Handshake(msg) => Arc::new(msg),
                RbfRibltMessageWrapper::BloomFilterSlice(msg) => Arc::new(msg),
                RbfRibltMessageWrapper::BloomSliceAck(msg) => Arc::new(msg),
                RbfRibltMessageWrapper::RBFStopSignal(msg) => Arc::new(msg),
                RbfRibltMessageWrapper::SendSymbol(msg) => Arc::new(msg),
                RbfRibltMessageWrapper::RequestMoreSymbols(msg) => Arc::new(msg),
                RbfRibltMessageWrapper::SComDecodedAll(msg) => Arc::new(msg),
                RbfRibltMessageWrapper::ValueFetchRequest(msg) => Arc::new(msg),
                RbfRibltMessageWrapper::ValueFetchResponse(msg) => Arc::new(msg),
            },
            Err(e) => {
                error!("Failed to deserialize RBF-RIBLT message: {}", e);
                Arc::new(TestMessage::new(Arc::new(TestMessageType::new()), None))
            }
        }
    }
}
