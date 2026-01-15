use async_trait::async_trait;
use dashmap::DashMap;

use std::marker::PhantomData;

use std::sync::Arc;

use connection::node::{
    NodeSocket, NodeSocketTask, NodeSocketTaskMetadata,
    iroh::{DefaultNodeSocketTask, DefaultNodeSocketTaskMetadata},
    port::NodePort,
};
use membership::{
    DefaultMembership, DefaultMembershipNeighbor, DefaultMembershipNeighborRepresentation,
    Membership, MembershipNeighbor, MembershipNeighbors,
};

#[async_trait]
pub trait NodeState<T, M, N, R, MN>
where
    T: NodeSocketTask<M>,
    M: NodeSocketTaskMetadata,
    N: Membership<R, MN>,
    R: MembershipNeighbors,
    MN: MembershipNeighbor,
{
    fn add_socket(
        &self,
        port: NodePort,
        socket: Box<dyn NodeSocket<T, M> + Send + Sync>,
    ) -> Result<(), String>;
    fn add_socket_task(&self, port: NodePort, task: Box<T>) -> Result<(), String>;
    fn add_socket_task_and_create(
        &self,
        port: NodePort,
        task: Box<T>,
        socket_constructor: Box<dyn Fn(NodePort) -> Box<dyn NodeSocket<T, M> + Send + Sync>>,
    );

    fn membership(&self) -> Arc<N>;

    async fn init(&self);
}

pub struct DefaultNodeState<T, M, R, N, MN>
where
    T: NodeSocketTask<M>,
    M: NodeSocketTaskMetadata,
    N: Membership<R, MN> + Send + Sync,
    R: MembershipNeighbors + Send + Sync,
    MN: MembershipNeighbor + Send + Sync,
{
    sockets: DashMap<NodePort, Box<dyn NodeSocket<T, M> + Send + Sync>>,
    membership: Arc<N>,
    _marker_r: PhantomData<R>,
    _marker_mn: PhantomData<MN>,
}

#[async_trait]
impl<T, M, R, N, MN> NodeState<T, M, N, R, MN> for DefaultNodeState<T, M, R, N, MN>
where
    T: NodeSocketTask<M>,
    M: NodeSocketTaskMetadata + Send + Sync,
    N: Membership<R, MN> + Send + Sync,
    R: MembershipNeighbors + Send + Sync,
    MN: MembershipNeighbor + Send + Sync,
{
    fn add_socket(
        &self,
        port: NodePort,
        socket: Box<dyn NodeSocket<T, M> + Send + Sync>,
    ) -> Result<(), String> {
        match self.sockets.insert(port.clone(), socket) {
            Some(_) => Ok(()),
            _ => Err(format!("Socket with port {} already exists", port.value())),
        }
    }

    fn membership(&self) -> Arc<N> {
        self.membership.clone()
    }

    fn add_socket_task_and_create(
        &self,
        port: NodePort,
        task: Box<T>,
        socket_constructor: Box<dyn Fn(NodePort) -> Box<dyn NodeSocket<T, M> + Send + Sync>>,
    ) {
        let element_exists = self.sockets.contains_key(&port);

        if !element_exists {
            self.add_socket(port.clone(), socket_constructor(port.clone()));
        }

        self.add_socket_task(port, task);
    }

    fn add_socket_task(&self, port: NodePort, task: Box<T>) -> Result<(), String> {
        match self.sockets.get_mut(&port) {
            Some(mut socket) => {
                socket.add_task(port, task);
                Ok(())
            }
            None => Err(format!("Socket with port {} not found", port.value())),
        }
    }

    async fn init(&self) {
        let keys = self
            .sockets
            .iter()
            .map(|x| x.key().clone())
            .collect::<Vec<NodePort>>();

        for key in keys {
            let socket = self.sockets.get_mut(&key).unwrap();

            socket.bind().await;
        }
    }
}

impl
    DefaultNodeState<
        DefaultNodeSocketTask,
        DefaultNodeSocketTaskMetadata,
        DefaultMembershipNeighborRepresentation,
        DefaultMembership,
        DefaultMembershipNeighbor,
    >
{
    pub fn new() -> Self {
        Self {
            sockets: DashMap::new(),
            membership: Arc::new(DefaultMembership::new()),
            _marker_r: PhantomData,
            _marker_mn: PhantomData,
        }
    }
}
