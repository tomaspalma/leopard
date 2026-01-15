#[derive(Eq, Hash, PartialEq, Clone)]
pub struct NodePort {
    value: u16
}

impl NodePort {
    pub fn new(value: u16) -> Self {
        Self {
            value
        }
    }

    pub fn value(&self) -> u16 {
        self.value
    }
}
