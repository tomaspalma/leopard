use crate::node::port::ConnectionInfo;
use crate::node::port::NodePort;

pub trait NodeIdentifier<T, V>
where
    T: ConnectionInfo<V>,
{
    fn connection_info(&self) -> T;
}

pub struct DefaultNodeIdentifier {
    port: NodePort,
}

impl DefaultNodeIdentifier {
    pub fn new(port: NodePort) -> Self {
        Self { port }
    }
}

impl NodeIdentifier<NodePort, u16> for DefaultNodeIdentifier {
    fn connection_info(&self) -> NodePort {
        self.port.clone()
    }
}
