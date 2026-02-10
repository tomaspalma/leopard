use connection::node::port::NodePort;
use membership::{
    DefaultMembershipNeighbor, DefaultMembershipNeighborRepresentation, Membership,
    MembershipNeighbor, MembershipNeighbors,
};
use std::sync::Arc;

pub trait NodeConfig<MN, N>
where
    MN: MembershipNeighbors<N>,
    N: MembershipNeighbor + Send + Sync,
{
    fn neighbors(&self) -> Arc<MN>;
}

pub struct DefaultNodeConfig {}

impl DefaultNodeConfig {
    pub fn new() -> Self {
        Self {}
    }
}

impl
    NodeConfig<
        DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>,
        DefaultMembershipNeighbor,
    > for DefaultNodeConfig
{
    fn neighbors(&self) -> Arc<DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>> {
        Arc::new(DefaultMembershipNeighborRepresentation::new(vec![
            Arc::new(DefaultMembershipNeighbor::new(NodePort::new(9000))),
            Arc::new(DefaultMembershipNeighbor::new(NodePort::new(9001))),
            Arc::new(DefaultMembershipNeighbor::new(NodePort::new(9002))),
        ]))
    }
}
