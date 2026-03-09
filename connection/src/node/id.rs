use crate::node::port::ConnectionInfo;
use crate::node::port::NodeAddress;

pub trait NodeIdentifier<T, V>
where
    T: ConnectionInfo<V>,
{
    fn connection_info(&self) -> T;
}

pub struct DefaultNodeIdentifier {
    address: NodeAddress,
}

impl DefaultNodeIdentifier {
    pub fn new(address: NodeAddress) -> Self {
        Self { address }
    }

    pub fn address(&self) -> NodeAddress {
        self.address.clone()
    }
}

impl NodeIdentifier<NodeAddress, NodeAddress> for DefaultNodeIdentifier {
    fn connection_info(&self) -> NodeAddress {
        self.address.clone()
    }
}
