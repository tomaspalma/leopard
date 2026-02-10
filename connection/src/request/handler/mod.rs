pub mod default;

pub trait RequestHandler {
    fn handle(&self);
}
