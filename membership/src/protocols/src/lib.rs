use connection::node::default::{DefaultNodeSocketTask, DefaultNodeSocketTaskMetadata};
use connection::node::port::NodePort;
use membership::{
    DefaultMembership, DefaultMembershipNeighbor, DefaultMembershipNeighborRepresentation,
};
use protocol::Protocol;
use state::node::DefaultNodeState;

pub struct DefaultMembershipProtocol {}

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
    > for DefaultMembershipProtocol
{
    fn init(&mut self) {}
}

impl DefaultMembershipProtocol {
    pub fn new() -> Self {
        Self {}
    }
}
