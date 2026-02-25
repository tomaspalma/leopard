use std::net::TcpStream;
use std::{io::Bytes, sync::Arc};

use message::{Message, MessageType, MessageTypeValues};

use crate::request::handler::RequestHandler;

pub struct DefaultRequestHandler {}

impl DefaultRequestHandler {
    pub fn new() -> Self {
        Self {}
    }
}

pub struct TestMessage {
    _type: Arc<dyn MessageType + Send + Sync>,
}

pub struct TestMessageTypeValues {}

impl TestMessageTypeValues {
    pub fn new() -> Self {
        Self {}
    }
}

impl MessageTypeValues for TestMessageTypeValues {}

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
    pub fn new(_type: Arc<dyn MessageType + Send + Sync>) -> Self {
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
}

impl RequestHandler<TcpStream> for DefaultRequestHandler {
    fn handle(&self, stream: Bytes<TcpStream>) -> Box<dyn Message + Send + Sync> {
        Box::new(TestMessage::new(Arc::new(TestMessageType::new())))
    }
}
