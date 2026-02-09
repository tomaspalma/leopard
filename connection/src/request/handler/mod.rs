pub trait RequestHandler {
    fn handle(&self);
}

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
