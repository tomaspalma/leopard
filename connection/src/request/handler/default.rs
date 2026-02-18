use std::io::Bytes;
use std::net::TcpStream;

use message::{DefaultMessage, DefaultMessageType, Message};

use crate::request::handler::RequestHandler;

pub struct DefaultRequestHandler {}

impl DefaultRequestHandler {
    pub fn new() -> Self {
        Self {}
    }
}

impl RequestHandler<DefaultMessage, DefaultMessageType, TcpStream> for DefaultRequestHandler {
    fn handle(&self, stream: Bytes<TcpStream>) -> Box<dyn Message<DefaultMessageType>> {
        Box::new(DefaultMessage::new())
    }
}
