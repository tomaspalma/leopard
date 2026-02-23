use async_trait::async_trait;
use connection::node::default::{
    DefaultNodeSocketTask, DefaultNodeSocketTaskMetadata, PeriodicDefaultNodeSocketTask,
};
use connection::node::port::NodePort;
use connection::route::{DefaultRouteHandler, HashMapRouteStorage};
use membership::{
    DefaultMembership, DefaultMembershipNeighbor, DefaultMembershipNeighborRepresentation,
};
use message::DefaultMessageType;
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
            DefaultMessageType,
            DefaultRouteHandler,
            HashMapRouteStorage,
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
        DefaultMessageType,
        DefaultRouteHandler,
        HashMapRouteStorage,
    > for DefaultMembershipProtocol
{
    async fn init(&mut self) {}
}

impl DefaultMembershipProtocol {
    pub fn new() -> Self {
        Self {}
    }
}
