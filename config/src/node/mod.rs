use connection::node::port::NodeAddress;
use membership::{
    DefaultMembershipNeighbor, DefaultMembershipNeighborRepresentation,
    MembershipNeighbor, MembershipNeighbors,
};
use std::sync::{Arc, RwLock};

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
        Arc::new(DefaultMembershipNeighborRepresentation::new(Arc::new(
            RwLock::new(vec![
                Arc::new(RwLock::new(DefaultMembershipNeighbor::new(
                    "node9000".to_string(),
                    NodeAddress::new("127.0.0.1".to_string(), 9000),
                ))),
                Arc::new(RwLock::new(DefaultMembershipNeighbor::new(
                    "node9001".to_string(),
                    NodeAddress::new("127.0.0.1".to_string(), 9001),
                ))),
                // Arc::new(RwLock::new(DefaultMembershipNeighbor::new(
                //     "node9002".to_string(),
                //     NodeAddress::new("127.0.0.1".to_string(), 9002),
                // ))),
            ]),
        )))
    }
}
