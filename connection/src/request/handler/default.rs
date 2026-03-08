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
    pub fn new(_type: Arc<TestMessageType>) -> Self {
        Self { _type }
    }
}

impl Message for TestMessage {
    fn content(&self) -> Arc<Vec<u8>> {
        Arc::new(vec![])
    }

    fn get_type(&self) -> Arc<dyn MessageType + Send + Sync> {
        self._type.clone()
    }

    fn serialize(&self) -> Result<Vec<u8>, ()> {
        let _bytes = rkyv::to_bytes::<Error>(self);

        match _bytes {
            Ok(bytes) => Ok(bytes.to_vec()),
            Err(_) => Err(()),
        }
    }
}

impl RequestHandler<Vec<u8>> for DefaultRequestHandler {
    fn handle(&self, stream: Vec<u8>) -> Arc<dyn Message + Send + Sync> {
        Arc::new(TestMessage::new(Arc::new(TestMessageType::new())))
    }
}
