use crate::node::port::ConnectionInfo;
use crate::node::port::NodeAddress;

pub trait NodeIdentifier<T, V>
where
    T: ConnectionInfo<V>,
{
    fn connection_info(&self) -> T;
}

pub struct DefaultNodeIdentifier {
    port: NodeAddress,
}

impl DefaultNodeIdentifier {
    pub fn new(port: NodeAddress) -> Self {
        Self { port }
    }
}

impl NodeIdentifier<NodeAddress, u16> for DefaultNodeIdentifier {
    fn connection_info(&self) -> NodeAddress {
        self.port.clone()
    }
}
