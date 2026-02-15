use async_trait::async_trait;
use connection::node::default::{
    DefaultNodeSocketTask, DefaultNodeSocketTaskMetadata, PeriodicDefaultNodeSocketTask,
};
use connection::node::port::NodePort;
use membership::{
    DefaultMembership, DefaultMembershipNeighbor, DefaultMembershipNeighborRepresentation,
};
use protocol::Protocol;
use runtime::time::TokioPeriodTimeUnit;
use state::node::DefaultNodeState;

pub struct DefaultMembershipProtocol {}

#[async_trait]
impl
    Protocol<
        DefaultNodeState<
            DefaultNodeSocketTask,
            DefaultNodeSocketTaskMetadata,
            DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>,
            DefaultMembership,
            DefaultMembershipNeighbor,
            NodePort,
            u16,
        >,
        DefaultNodeSocketTask,
        DefaultNodeSocketTaskMetadata,
        DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>,
        DefaultMembership,
        DefaultMembershipNeighbor,
        NodePort,
        u16,
        TokioPeriodTimeUnit,
        PeriodicDefaultNodeSocketTask,
    > for DefaultMembershipProtocol
{
    async fn init(&mut self) {}
}

impl DefaultMembershipProtocol {
    pub fn new() -> Self {
        Self {}
    }
}
