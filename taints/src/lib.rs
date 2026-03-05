use connection::node::port::NodeAddress;

pub trait Taint {
    fn tainted(&self) -> bool;
}

pub struct NodeAddressTaint {
    port: NodeAddress,
    other_port: NodeAddress,
}

impl NodeAddressTaint {
    pub fn new(port: NodeAddress, other_port: NodeAddress) -> Self {
        Self { port, other_port }
    }
}

impl Taint for NodeAddressTaint {
    fn tainted(&self) -> bool {
        self.port == self.other_port
    }
}
