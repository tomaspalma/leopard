pub struct Riblt {
    iblt: Iblt
}

impl Protocol for Riblt {
    fn init(&self, config: ProtocolConfig) -> Self {
        Self {}
    }
}