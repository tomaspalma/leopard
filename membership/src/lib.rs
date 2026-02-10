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

pub trait MembershipNeighbors<N>
where
    N: MembershipNeighbor + Send + Sync,
{
    fn neighbors(&self) -> Vec<Arc<N>>;
}

pub struct DefaultMembershipNeighborRepresentation<N>
where
    N: MembershipNeighbor + Send + Sync,
{
    neighbors: Vec<Arc<N>>,
}

impl<N> DefaultMembershipNeighborRepresentation<N>
where
    N: MembershipNeighbor + Send + Sync,
{
    pub fn new(neighbors: Vec<Arc<N>>) -> Self {
        Self { neighbors }
    }
}

impl MembershipNeighbors<DefaultMembershipNeighbor>
    for DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>
{
    fn neighbors(&self) -> Vec<Arc<DefaultMembershipNeighbor>> {
        self.neighbors.clone()
    }
}

pub trait Membership<R, N>
where
    R: MembershipNeighbors<N>,
    N: MembershipNeighbor + Send + Sync,
{
    fn neighbors(&self) -> R;
    fn add_neighbor(&self, neighbor: Arc<N>);
    fn add_multiple_neighbors(&self, neighbors: Arc<R>) {
        for neighbor in neighbors.neighbors() {
            self.add_neighbor(neighbor);
        }
    }
}

pub struct DefaultMembership {}

impl DefaultMembership {
    pub fn new() -> Self {
        Self {}
    }
}

impl
    Membership<
        DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>,
        DefaultMembershipNeighbor,
    > for DefaultMembership
{
    fn neighbors(&self) -> DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor> {
        DefaultMembershipNeighborRepresentation { neighbors: vec![] }
    }

    fn add_neighbor(&self, neighbor: Arc<DefaultMembershipNeighbor>) {
        println!("fds meu: {}", self.neighbors().neighbors().len());
        self.neighbors().neighbors().push(neighbor);
    }
}
