use connection::request::handler::default::{TestMessage, TestMessageType};
use message::Message;
use protocol::deserializer::ProtocolDeserializer;
use rkyv::{from_bytes, rancor::Error};
use std::sync::Arc;
use tracing::error;

use crate::messages::{
    ReplicationMessage, ReplicationMessageType, ReplicationMessageTypeValues,
    ReplicationMessageWrapper,
};

pub struct ReplicationDeserializer {}

impl ReplicationDeserializer {
    pub fn new() -> Self {
        Self {}
    }
}

impl ProtocolDeserializer for ReplicationDeserializer {
    fn deserialize(&self, bytes: Vec<u8>) -> Arc<dyn Message + Send + Sync> {
        if bytes.len() < 16 {
            return Arc::new(TestMessage::new(Arc::new(TestMessageType::new()), None));
        }

        let mut protocol_id_bytes = [0; 8];
        protocol_id_bytes.copy_from_slice(&bytes[0..8]);
        let protocol_id = u64::from_be_bytes(protocol_id_bytes);
        let protocol_id = if protocol_id == 0 {
            None
        } else {
            Some(protocol_id)
        };

        let body_bytes = &bytes[16..];

        match from_bytes::<ReplicationMessageWrapper, Error>(body_bytes) {
            Ok(wrapper) => {
                let msg_type_val = match &wrapper {
                    ReplicationMessageWrapper::InsertNotification(_, _) => {
                        ReplicationMessageTypeValues::InsertNotification
                    }
                };
                Arc::new(ReplicationMessage::new(
                    protocol_id,
                    ReplicationMessageType::new(msg_type_val),
                    wrapper,
                ))
            }
            Err(e) => {
                error!("Failed to deserialize ReplicationMessage: {}", e);
                Arc::new(TestMessage::new(Arc::new(TestMessageType::new()), None))
            }
        }
    }
}
