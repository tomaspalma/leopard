use std::io::Bytes;
use std::net::TcpStream;

use std::rc::Rc;

use message::{Message, MessageType, MessageTypeValues};

use crate::request::handler::RequestHandler;

pub struct DefaultRequestHandler {}

impl DefaultRequestHandler {
    pub fn new() -> Self {
        Self {}
    }
}

pub struct TestMessage {
    _type: Rc<dyn MessageType>,
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
    pub fn new(_type: Rc<dyn MessageType>) -> Self {
        Self { _type }
    }
}

impl Message for TestMessage {
    fn content(&self) -> Rc<Vec<u8>> {
        Rc::new(vec![])
    }

    fn get_type(&self) -> Rc<dyn MessageType> {
        self._type.clone()
    }
}

impl RequestHandler<TcpStream> for DefaultRequestHandler {
    fn handle(&self, stream: Bytes<TcpStream>) -> Box<dyn Message> {
        Box::new(TestMessage::new(Rc::new(TestMessageType::new())))
    }
}
