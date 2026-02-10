use std::sync::{Arc, RwLock};

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
    fn neighbors(&self) -> Arc<RwLock<Vec<Arc<N>>>>;
}

pub struct DefaultMembershipNeighborRepresentation<N>
where
    N: MembershipNeighbor + Send + Sync,
{
    neighbors: Arc<RwLock<Vec<Arc<N>>>>,
}

impl<N> DefaultMembershipNeighborRepresentation<N>
where
    N: MembershipNeighbor + Send + Sync,
{
    pub fn new(neighbors: Arc<RwLock<Vec<Arc<N>>>>) -> Self {
        Self { neighbors }
    }
}

impl MembershipNeighbors<DefaultMembershipNeighbor>
    for DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>
{
    fn neighbors(&self) -> Arc<RwLock<Vec<Arc<DefaultMembershipNeighbor>>>> {
        self.neighbors.clone()
    }
}

pub trait Membership<R, N>
where
    R: MembershipNeighbors<N>,
    N: MembershipNeighbor + Send + Sync,
{
    fn neighbors(&self) -> Arc<R>;
    fn add_neighbor(&self, neighbor: Arc<N>);
    fn add_multiple_neighbors(&self, new_neighbors: Vec<Arc<N>>) {
        for i in 0..new_neighbors.len() {
            self.add_neighbor(new_neighbors[i].clone());
        }
    }
}

pub struct DefaultMembership {
    neighbors: Arc<DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>>,
}

impl DefaultMembership {
    pub fn new() -> Self {
        Self {
            neighbors: Arc::new(DefaultMembershipNeighborRepresentation::new(Arc::new(
                RwLock::new(Vec::new()),
            ))),
        }
    }
}

impl
    Membership<
        DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>,
        DefaultMembershipNeighbor,
    > for DefaultMembership
{
    fn neighbors(&self) -> Arc<DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>> {
        self.neighbors.clone()
    }

    fn add_neighbor(&self, neighbor: Arc<DefaultMembershipNeighbor>) {
        self.neighbors().neighbors().write().unwrap().push(neighbor);
    }
}
