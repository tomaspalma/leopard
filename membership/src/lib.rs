use std::sync::{Arc, RwLock};

use connection::node::{id::NodeIdentifier, port::NodePort};
use taints::Taint;

pub trait MembershipNeighbor {
    fn add_taint(&mut self, taint: Box<dyn Taint + Send + Sync>);
    fn identifier(&self) -> Arc<dyn NodeIdentifier<NodePort, u16> + Send + Sync>;
}

pub struct DefaultMembershipNeighbor {
    identifier: Arc<dyn NodeIdentifier<NodePort, u16> + Send + Sync>,
    taints: Vec<Box<dyn Taint + Send + Sync>>,
}

impl DefaultMembershipNeighbor {
    pub fn new(port: NodePort) -> Self {
        Self {
            identifier: Arc::new(connection::node::id::DefaultNodeIdentifier::new(port)),
            taints: Vec::new(),
        }
    }
}

impl MembershipNeighbor for DefaultMembershipNeighbor {
    fn add_taint(&mut self, taint: Box<dyn Taint + Send + Sync>) {
        self.taints.push(taint);
    }

    fn identifier(&self) -> Arc<dyn NodeIdentifier<NodePort, u16> + Send + Sync> {
        Arc::new(connection::node::id::DefaultNodeIdentifier::new(
            NodePort::new(9000),
        ))
    }
}

pub trait MembershipNeighbors<N>
where
    N: MembershipNeighbor + Send + Sync,
{
    fn neighbors(&self) -> Arc<RwLock<Vec<Arc<RwLock<N>>>>>;
}

pub struct DefaultMembershipNeighborRepresentation<N>
where
    N: MembershipNeighbor + Send + Sync,
{
    neighbors: Arc<RwLock<Vec<Arc<RwLock<N>>>>>,
}

impl<N> DefaultMembershipNeighborRepresentation<N>
where
    N: MembershipNeighbor + Send + Sync,
{
    pub fn new(neighbors: Arc<RwLock<Vec<Arc<RwLock<N>>>>>) -> Self {
        Self { neighbors }
    }
}

impl MembershipNeighbors<DefaultMembershipNeighbor>
    for DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>
{
    fn neighbors(&self) -> Arc<RwLock<Vec<Arc<RwLock<DefaultMembershipNeighbor>>>>> {
        self.neighbors.clone()
    }
}

pub trait Membership<R, N>
where
    R: MembershipNeighbors<N>,
    N: MembershipNeighbor + Send + Sync,
{
    fn neighbors(&self) -> Arc<R>;
    fn add_neighbor(&self, neighbor: Arc<RwLock<N>>);
    fn add_multiple_neighbors(&self, new_neighbors: Vec<Arc<RwLock<N>>>) {
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

    fn add_neighbor(&self, neighbor: Arc<RwLock<DefaultMembershipNeighbor>>) {
        self.neighbors().neighbors().write().unwrap().push(neighbor);
    }
}
