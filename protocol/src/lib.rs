pub trait Protocol {
    fn init(&self, config: ProtocolConfig) -> Self;
}