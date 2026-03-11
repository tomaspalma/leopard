use async_trait::async_trait;
use connection::node::default::{
    DefaultNodeSocketTask, DefaultNodeSocketTaskMetadata, PeriodicDefaultNodeSocketTask,
};
use connection::node::port::NodeAddress;
use connection::route::default::{DefaultRouteHandler, HashMapRouteStorage};
use membership::{
    DefaultMembership, DefaultMembershipNeighbor, DefaultMembershipNeighborRepresentation,
};
use protocol::{Protocol, ProtocolIDGenerator};
use runtime::time::TokioPeriodTimeUnit;
use state::node::DefaultNodeState;

pub struct DefaultMembershipProtocol {
    id: u64,
}

impl DefaultMembershipProtocol {
    pub fn new() -> Self {
        Self {
            id: ProtocolIDGenerator::generate(),
        }
    }
}

#[async_trait]
impl
    Protocol<
        DefaultNodeState,
        DefaultNodeSocketTask,
        DefaultNodeSocketTaskMetadata,
        DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>,
        DefaultMembership,
        DefaultMembershipNeighbor,
        NodeAddress,
        NodeAddress,
        TokioPeriodTimeUnit,
        PeriodicDefaultNodeSocketTask,
        DefaultRouteHandler,
        HashMapRouteStorage,
    > for DefaultMembershipProtocol
{
    fn id(&self) -> u64 {
        self.id
    }
    async fn init(&mut self) {}
}
