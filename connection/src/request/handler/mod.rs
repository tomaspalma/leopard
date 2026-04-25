

pub mod default;

pub trait RequestHandler<SType, RType> {
    fn handle(&self, stream: SType) -> RType;
}
