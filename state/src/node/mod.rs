use async_trait::async_trait;
use config::node::NodeConfig;
use dashmap::DashMap;
use errors::node::NodeInitError;
use runtime::Runtime;

use std::marker::PhantomData;

use std::sync::{Arc, RwLock};

use connection::node::{
    NodeSocket, NodeSocketTask, NodeSocketTaskMetadata,
    default::{DefaultNodeSocketTask, DefaultNodeSocketTaskMetadata},
    id::{DefaultNodeIdentifier, NodeIdentifier},
    port::{ConnectionInfo, NodePort},
};
use membership::{
    DefaultMembership, DefaultMembershipNeighbor, DefaultMembershipNeighborRepresentation,
    Membership, MembershipNeighbor, MembershipNeighbors,
};

#[async_trait]
pub trait NodeState<T, M, N, R, MN, CI, CV>
where
    T: NodeSocketTask<M>,
    M: NodeSocketTaskMetadata,
    N: Membership<R, MN>,
    R: MembershipNeighbors<MN>,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV>,
    CV: Sized,
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

    fn membership(&self) -> Arc<RwLock<N>>;

    fn init_neighbors(&self);

    async fn init(&self) -> Result<(), NodeInitError>;
}

pub struct DefaultNodeState<T, M, R, N, MN, CI, CV>
where
    T: NodeSocketTask<M>,
    M: NodeSocketTaskMetadata,
    N: Membership<R, MN> + Send + Sync,
    R: MembershipNeighbors<MN> + Send + Sync,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV> + Send + Sync,
    CV: Sized,
{
    sockets: DashMap<NodePort, Box<dyn NodeSocket<T, M> + Send + Sync>>,
    membership: Arc<RwLock<N>>,
    config: Arc<dyn NodeConfig<R, MN> + Send + Sync>,
    runtime: Arc<dyn Runtime + Sync + Send>,
    identifier: Box<dyn NodeIdentifier<CI, CV> + Send + Sync>,
    _marker_r: PhantomData<R>,
    _marker_mn: PhantomData<MN>,
}

#[async_trait]
impl<T, M, R, N, MN, CI, CV> NodeState<T, M, N, R, MN, CI, CV>
    for DefaultNodeState<T, M, R, N, MN, CI, CV>
where
    T: NodeSocketTask<M>,
    M: NodeSocketTaskMetadata + Send + Sync,
    N: Membership<R, MN> + Send + Sync,
    R: MembershipNeighbors<MN> + Send + Sync,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV> + Send + Sync,
    CV: Sized,
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

    fn membership(&self) -> Arc<RwLock<N>> {
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

    fn init_neighbors(&self) {
        self.membership
            .write()
            .unwrap()
            .add_multiple_neighbors(self.config.neighbors().neighbors().read().unwrap().clone());
    }

    async fn init(&self) -> Result<(), NodeInitError> {
        self.init_neighbors();

        let keys = self
            .sockets
            .iter()
            .map(|x| x.key().clone())
            .collect::<Vec<NodePort>>();

        for key in keys {
            let socket = self.sockets.get_mut(&key);

            if let None = socket {
                return Err(NodeInitError::SocketDoesNotExist());
            }

            let listener = socket.unwrap().bind().await?;
        }

        Ok(())
    }
}

impl
    DefaultNodeState<
        DefaultNodeSocketTask,
        DefaultNodeSocketTaskMetadata,
        DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>,
        DefaultMembership,
        DefaultMembershipNeighbor,
        NodePort,
        u16,
    >
{
    pub fn new(
        runtime: Arc<dyn Runtime + Sync + Send>,
        config: Arc<
            dyn NodeConfig<
                    DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>,
                    DefaultMembershipNeighbor,
                > + Send
                + Sync,
        >,
        identifier: Box<dyn NodeIdentifier<NodePort, u16> + Send + Sync>,
    ) -> Self {
        Self {
            sockets: DashMap::new(),
            membership: Arc::new(RwLock::new(DefaultMembership::new())),
            config,
            runtime,
            identifier,
            _marker_r: PhantomData,
            _marker_mn: PhantomData,
        }
    }
}
