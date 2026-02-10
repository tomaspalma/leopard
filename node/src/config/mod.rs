use connection::node::port::NodePort;
use membership::{
    DefaultMembershipNeighbor, DefaultMembershipNeighborRepresentation, Membership,
    MembershipNeighbors,
};
use std::sync::Arc;

pub trait NodeConfig {
    fn neighbors(&self) -> Box<dyn MembershipNeighbors>;
}

pub struct DefaultNodeConfig {}

impl NodeConfig for DefaultNodeConfig {
    fn neighbors(&self) -> Box<dyn MembershipNeighbors> {
        Box::new(DefaultMembershipNeighborRepresentation::new(vec![
            Arc::new(DefaultMembershipNeighbor::new(NodePort::new(9000))),
            Arc::new(DefaultMembershipNeighbor::new(NodePort::new(9001))),
            Arc::new(DefaultMembershipNeighbor::new(NodePort::new(9002))),
        ]))
    }
}
