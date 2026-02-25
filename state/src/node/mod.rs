use async_trait::async_trait;
use config::node::NodeConfig;
use connection::node::default::NodeSocketRoute;
use dashmap::DashMap;
use errors::node::NodeInitError;
use message::MessageType;
use runtime::time::TokioPeriodTimeUnit;
use runtime::{Runtime, time::PeriodTimeUnit};

use std::marker::PhantomData;

use std::sync::{Arc, RwLock};

use connection::route::{
    DefaultRouteHandler, HashMapRouteStorage, NodeSocketRouteId, RouteHandler, RouteStorage,
    RouteTask,
};

use connection::node::{
    NodeSocket, NodeSocketTaskMetadata, PeriodicNodeSocketTask,
    default::{
        DefaultNodeSocketTask, DefaultNodeSocketTaskMetadata, PeriodicDefaultNodeSocketTask,
    },
    id::NodeIdentifier,
    port::{ConnectionInfo, NodePort},
};
use membership::{
    DefaultMembership, DefaultMembershipNeighbor, DefaultMembershipNeighborRepresentation,
    Membership, MembershipNeighbor, MembershipNeighbors,
};
use taints::NodePortTaint;

#[async_trait]
pub trait NodeState<T, M, N, R, MN, CI, CV, PTU, PT, RHandler, RStorage>
where
    T: RouteTask,
    M: NodeSocketTaskMetadata,
    N: Membership<R, MN>,
    R: MembershipNeighbors<MN>,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV>,
    CV: Sized,
    PTU: PeriodTimeUnit + Send + Sync,
    PT: PeriodicNodeSocketTask<PTU>,
    RHandler: RouteHandler<RStorage> + Send + Sync,
    RStorage: RouteStorage,
{
    type RouteId;

    fn add_socket(
        &self,
        port: NodePort,
        socket: Box<
            dyn NodeSocket<
                    T,
                    PeriodicDefaultNodeSocketTask,
                    PTU,
                    M,
                    RStorage,
                    RouteId = Self::RouteId,
                > + Send
                + Sync,
        >,
    ) -> Result<(), String>;
    async fn add_periodic_socket_task(&self, port: NodePort, task: Arc<PT>) -> Result<(), String>;
    fn add_socket_task_and_create(
        &self,
        id: Self::RouteId,
        task: Box<T>,
        socket_constructor: Box<
            dyn Fn(
                NodePort,
            ) -> Box<
                dyn NodeSocket<
                        T,
                        PeriodicDefaultNodeSocketTask,
                        PTU,
                        M,
                        RStorage,
                        RouteId = Self::RouteId,
                    > + Send
                    + Sync,
            >,
        >,
    ) -> Result<(), String>;

    fn route_handler(&self) -> Arc<RHandler>;

    fn add_socket_task(&self, id: Self::RouteId, task: Box<T>) -> Result<(), String>;

    fn node_identifier(&self) -> Arc<dyn NodeIdentifier<CI, CV> + Send + Sync>;

    fn membership(&self) -> Arc<RwLock<N>>;

    fn init_neighbors(&self);

    async fn init(&self) -> Result<(), NodeInitError>;
}

pub struct DefaultNodeState<T, M, R, N, MN, CI, CV, RHandler, RStorage>
where
    T: RouteTask,
    M: NodeSocketTaskMetadata,
    N: Membership<R, MN> + Send + Sync,
    R: MembershipNeighbors<MN> + Send + Sync,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV> + Send + Sync,
    CV: Sized,
    RHandler: RouteHandler<RStorage> + Send + Sync,
    RStorage: RouteStorage,
{
    sockets: DashMap<
        NodePort,
        Box<
            dyn NodeSocket<
                    T,
                    PeriodicDefaultNodeSocketTask,
                    TokioPeriodTimeUnit,
                    M,
                    RStorage,
                    RouteId = NodeSocketRouteId,
                > + Send
                + Sync,
        >,
    >,
    membership: Arc<RwLock<N>>,
    config: Arc<dyn NodeConfig<R, MN> + Send + Sync>,
    identifier: Arc<dyn NodeIdentifier<CI, CV> + Send + Sync>,
    route_handler: Arc<RHandler>,
    _marker_r: PhantomData<R>,
    _marker_mn: PhantomData<MN>,
}

#[async_trait]
impl<T, M, R, N, MN>
    NodeState<
        T,
        M,
        N,
        R,
        MN,
        NodePort,
        u16,
        TokioPeriodTimeUnit,
        PeriodicDefaultNodeSocketTask,
        DefaultRouteHandler,
        HashMapRouteStorage,
    > for DefaultNodeState<T, M, R, N, MN, NodePort, u16, DefaultRouteHandler, HashMapRouteStorage>
where
    T: RouteTask + Send + Sync + 'static,
    M: NodeSocketTaskMetadata + Send + Sync,
    N: Membership<R, MN> + Send + Sync,
    R: MembershipNeighbors<MN> + Send + Sync,
    MN: MembershipNeighbor + Send + Sync,
{
    type RouteId = NodeSocketRouteId;

    fn add_socket(
        &self,
        port: NodePort,
        socket: Box<
            dyn NodeSocket<
                    T,
                    PeriodicDefaultNodeSocketTask,
                    TokioPeriodTimeUnit,
                    M,
                    HashMapRouteStorage,
                    RouteId = NodeSocketRouteId,
                > + Send
                + Sync,
        >,
    ) -> Result<(), String> {
        self.sockets.insert(port.clone(), socket);
        Ok(())
    }

    fn route_handler(&self) -> Arc<DefaultRouteHandler> {
        self.route_handler.clone()
    }

    fn node_identifier(&self) -> Arc<dyn NodeIdentifier<NodePort, u16> + Send + Sync> {
        self.identifier.clone()
    }

    fn membership(&self) -> Arc<RwLock<N>> {
        self.membership.clone()
    }

    fn add_socket_task_and_create(
        &self,
        id: NodeSocketRouteId,
        task: Box<T>,
        socket_constructor: Box<
            dyn Fn(
                NodePort,
            ) -> Box<
                dyn NodeSocket<
                        T,
                        PeriodicDefaultNodeSocketTask,
                        TokioPeriodTimeUnit,
                        M,
                        HashMapRouteStorage,
                        RouteId = NodeSocketRouteId,
                    > + Send
                    + Sync,
            >,
        >,
    ) -> Result<(), String> {
        let element_exists = self.sockets.contains_key(&id.info().port());

        if !element_exists {
            self.add_socket(id.info().port(), socket_constructor(id.info().port()))?;
        }

        self.add_socket_task(id, task)?;

        Ok(())
    }

    async fn add_periodic_socket_task(
        &self,
        port: NodePort,
        task: Arc<PeriodicDefaultNodeSocketTask>,
    ) -> Result<(), String> {
        match self.sockets.get_mut(&port) {
            Some(mut socket) => {
                socket.add_periodic_task(task).await;
                Ok(())
            }
            None => Err(format!("Socket with port {} not found", port.value())),
        }
    }

    fn add_socket_task(&self, id: NodeSocketRouteId, task: Box<T>) -> Result<(), String> {
        self.route_handler()
            .add_route(id, Arc::new(NodeSocketRoute::new(task)));

        Ok(())
    }

    fn init_neighbors(&self) {
        let neighbors = self.config.neighbors().neighbors().read().unwrap().clone();

        for i in 0..neighbors.len() {
            let mut n = neighbors[i].write().unwrap();

            let neighbor_info = self.node_identifier().connection_info();

            if self.node_identifier().connection_info().value() == neighbor_info.value() {
                n.add_taint(Box::new(NodePortTaint::new(
                    self.node_identifier().connection_info(),
                    neighbor_info,
                )));
            }

            self.membership
                .write()
                .unwrap()
                .add_neighbor(neighbors[i].clone());
        }
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
        DefaultRouteHandler,
        HashMapRouteStorage,
    >
{
    pub fn new(
        config: Arc<
            dyn NodeConfig<
                    DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>,
                    DefaultMembershipNeighbor,
                > + Send
                + Sync,
        >,
        identifier: Arc<dyn NodeIdentifier<NodePort, u16> + Send + Sync>,
        route_handler: Arc<DefaultRouteHandler>,
    ) -> Self {
        Self {
            sockets: DashMap::new(),
            membership: Arc::new(RwLock::new(DefaultMembership::new())),
            config,
            identifier,
            route_handler,
            _marker_r: PhantomData,
            _marker_mn: PhantomData,
        }
    }
}
