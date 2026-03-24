use rkyv::rancor::Error;
use rkyv::{Archive, Deserialize, Serialize};
use std::sync::Arc;

use std::mem::size_of;

use message::{Message, MessageType, MessageTypeValues};

use crate::request::handler::RequestHandler;

pub struct DefaultRequestHandler {}

impl DefaultRequestHandler {
    pub fn new() -> Self {
        Self {}
    }
}

#[derive(Archive, Serialize, Deserialize)]
pub struct TestMessage {
    _type: Arc<TestMessageType>,
    protocol: Option<u64>,
}

#[derive(Archive, Serialize, Deserialize)]
pub struct TestMessageTypeValues {}

impl TestMessageTypeValues {
    pub fn new() -> Self {
        Self {}
    }
}

impl MessageTypeValues for TestMessageTypeValues {}

#[derive(Archive, Serialize, Deserialize)]
pub struct TestMessageType {}

impl TestMessageType {
    pub fn new() -> Self {
        Self {}
    }
}

impl MessageType for TestMessageType {
    fn value(&self) -> Box<dyn MessageTypeValues> {
        Box::new(TestMessageTypeValues::new())
    }
}

impl TestMessage {
    pub fn new(_type: Arc<TestMessageType>, protocol: Option<u64>) -> Self {
        Self { _type, protocol }
    }
}

impl Message for TestMessage {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn content(&self) -> Arc<Vec<u8>> {
        Arc::new(vec![])
    }

    fn get_type(&self) -> Arc<dyn MessageType + Send + Sync> {
        self._type.clone()
    }

    fn protocol(&self) -> Option<u64> {
        self.protocol
    }

    fn serialize(&self, protocol: Option<u64>) -> Result<Vec<u8>, ()> {
        let body_bytes = rkyv::to_bytes::<Error>(self).map_err(|_| ())?;

        let mut packet = Vec::with_capacity(body_bytes.len() + 8);

        if let Some(id) = protocol {
            packet.extend_from_slice(&id.to_be_bytes());
        }

        packet.extend_from_slice(&body_bytes);

        Ok(packet)
    }
}

impl RequestHandler<Vec<u8>, u64> for DefaultRequestHandler {
    fn handle(&self, stream: Vec<u8>) -> u64 {
        if stream.len() < size_of::<u64>() {
            return 0;
        }

        let (header, _payload) = stream.split_at(8);

        u64::from_be_bytes(header.try_into().unwrap())
    }
}
