use connection::node::port::NodePort;

pub trait Taint {
    fn tainted(&self) -> bool;
}

pub struct NodePortTaint {
    port: NodePort,
    other_port: NodePort,
}

impl NodePortTaint {
    pub fn new(port: NodePort, other_port: NodePort) -> Self {
        Self { port, other_port }
    }
}

impl Taint for NodePortTaint {
    fn tainted(&self) -> bool {
        self.port == self.other_port
    }
}
