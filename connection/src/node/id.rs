use crate::node::port::ConnectionInfo;
use crate::node::port::NodeAddress;

pub trait NodeIdentifier<T, V>
where
    T: ConnectionInfo<V>,
{
    fn id(&self) -> String;
    fn connection_info(&self) -> T;
}

pub struct DefaultNodeIdentifier {
    id: String,
    address: NodeAddress,
}

impl DefaultNodeIdentifier {
    pub fn new(id: String, address: NodeAddress) -> Self {
        Self { id, address }
    }

    pub fn address(&self) -> NodeAddress {
        self.address.clone()
    }
}

impl NodeIdentifier<NodeAddress, NodeAddress> for DefaultNodeIdentifier {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn connection_info(&self) -> NodeAddress {
        self.address.clone()
    }
}
