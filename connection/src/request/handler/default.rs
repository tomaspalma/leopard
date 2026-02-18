use message::{DefaultMessage, DefaultMessageType};

use crate::request::handler::RequestHandler;

pub struct DefaultRequestHandler {}

impl DefaultRequestHandler {
    pub fn new() -> Self {
        Self {}
    }
}

impl RequestHandler<DefaultMessage, DefaultMessageType> for DefaultRequestHandler {
    fn handle(&self) {
        println!("Handling request");
    }
}
