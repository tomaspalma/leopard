use crate::request::handler::RequestHandler;

pub struct DefaultRequestHandler {}

impl DefaultRequestHandler {
    pub fn new() -> Self {
        Self {}
    }
}

impl RequestHandler for DefaultRequestHandler {
    fn handle(&self) {
        println!("Handling request");
    }
}
