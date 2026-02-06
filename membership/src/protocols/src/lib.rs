use connection::node::iroh::{DefaultNodeSocketTask, DefaultNodeSocketTaskMetadata};
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
            DefaultMembershipNeighborRepresentation,
            DefaultMembership,
            DefaultMembershipNeighbor,
        >,
        DefaultNodeSocketTask,
        DefaultNodeSocketTaskMetadata,
        DefaultMembershipNeighborRepresentation,
        DefaultMembership,
        DefaultMembershipNeighbor,
    > for DefaultMembershipProtocol
{
    fn init(&mut self) {}
}

impl DefaultMembershipProtocol {
    pub fn new() -> Self {
        Self {}
    }
}
