use rkyv::{Archive, Deserialize, Serialize, rancor::Error};
use std::any::Any;

use message::{impl_protocol_message, MessageType, MessageTypeValues};

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub enum ReplicationMessageWrapper {
    InsertNotification(String, String),
}

#[derive(Debug, Clone)]
pub enum ReplicationMessageTypeValues {
    InsertNotification,
}

impl MessageTypeValues for ReplicationMessageTypeValues {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct ReplicationMessageType {
    value: ReplicationMessageTypeValues,
}

impl ReplicationMessageType {
    pub fn new(value: ReplicationMessageTypeValues) -> Self {
        Self { value }
    }
}

impl MessageType for ReplicationMessageType {
    fn value(&self) -> Box<dyn MessageTypeValues> {
        Box::new(self.value.clone())
    }
}

#[derive(Debug, Clone)]
pub struct ReplicationMessage {
    protocol_id: Option<u64>,
    _type: ReplicationMessageType,
    wrapper: ReplicationMessageWrapper,
}

impl ReplicationMessage {
    pub fn new(
        protocol_id: Option<u64>,
        _type: ReplicationMessageType,
        wrapper: ReplicationMessageWrapper,
    ) -> Self {
        Self {
            protocol_id,
            _type,
            wrapper,
        }
    }

    pub fn wrapper(&self) -> &ReplicationMessageWrapper {
        &self.wrapper
    }
}

impl_protocol_message!(ReplicationMessage, this, {
    rkyv::to_bytes::<Error>(&this.wrapper.clone()).map_err(|_| ())?
});
