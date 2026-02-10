use std::sync::Arc;

use connection::node::port::NodePort;

pub trait MembershipNeighbor {}

pub struct DefaultMembershipNeighbor {
    port: NodePort,
}

impl DefaultMembershipNeighbor {
    pub fn new(port: NodePort) -> Self {
        Self { port }
    }
}

impl MembershipNeighbor for DefaultMembershipNeighbor {}

pub trait MembershipNeighbors {
    fn neighbors(&self) -> Vec<Arc<dyn MembershipNeighbor + Send + Sync>>;
}

pub struct DefaultMembershipNeighborRepresentation {
    neighbors: Vec<Arc<dyn MembershipNeighbor + Send + Sync>>,
}

impl DefaultMembershipNeighborRepresentation {
    pub fn new(neighbors: Vec<Arc<dyn MembershipNeighbor + Send + Sync>>) -> Self {
        Self { neighbors }
    }
}

impl MembershipNeighbors for DefaultMembershipNeighborRepresentation {
    fn neighbors(&self) -> Vec<Arc<dyn MembershipNeighbor + Send + Sync>> {
        self.neighbors.clone()
    }
}

pub trait Membership<R, N>
where
    R: MembershipNeighbors,
    N: MembershipNeighbor,
{
    fn neighbors(&self) -> R;
    fn add_neighbor(&self, neighbor: Arc<N>);
}

pub struct DefaultMembership {}

impl DefaultMembership {
    pub fn new() -> Self {
        Self {}
    }
}

impl Membership<DefaultMembershipNeighborRepresentation, DefaultMembershipNeighbor>
    for DefaultMembership
{
    fn neighbors(&self) -> DefaultMembershipNeighborRepresentation {
        DefaultMembershipNeighborRepresentation { neighbors: vec![] }
    }

    fn add_neighbor(&self, neighbor: Arc<DefaultMembershipNeighbor>) {}
}
