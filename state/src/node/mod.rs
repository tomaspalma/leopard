use crate::storage::{DataState, DefaultDataState, DefaultDataStateItem};

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
    port::{ConnectionInfo, NodeAddress},
};
use membership::{
    DefaultMembership, DefaultMembershipNeighbor, DefaultMembershipNeighborRepresentation,
    Membership, MembershipNeighbor, MembershipNeighbors,
};
use taints::NodeAddressTaint;

#[async_trait]
pub trait NodeState {
    type RouteTask;
    type NodeSocketTaskMetadata;
    type Membership;
    type MembershipNeighbor;
    type MembershipNeighborRepresentation;
    type ConnectionValue;
    type PeriodTimeUnit;
    type PeriodicNodeSocketTask;
    type RouteHandler;
    type RouteStorage;

    type RouteId;
    type ConnectionInfo;
    type StreamType;

    fn add_socket(
        &self,
        port: NodeAddress,
        socket: Box<
            dyn NodeSocket<
                    RouteTask = Self::RouteTask,
                    NodeSocketTaskMetadata = Self::NodeSocketTaskMetadata,
                    PeriodicNodeSocketTask = Self::PeriodicNodeSocketTask,
                    PeriodTimeUnit = Self::PeriodTimeUnit,
                    RouteStorage = Self::RouteStorage,
                    RouteId = Self::RouteId,
                    ConnectionInfo = Self::ConnectionInfo,
                    StreamType = Self::StreamType,
                > + Send
                + Sync,
        >,
    ) -> Result<(), String>;
    async fn add_periodic_socket_task(
        &self,
        port: NodeAddress,
        task: Arc<Self::PeriodicNodeSocketTask>,
    ) -> Result<(), String>;
    fn add_socket_task_and_create(
        &self,
        id: Self::RouteId,
        task: Box<Self::RouteTask>,
        socket_constructor: Box<
            dyn Fn(
                NodeAddress,
            ) -> Box<
                dyn NodeSocket<
                        RouteTask = Self::RouteTask,
                        NodeSocketTaskMetadata = Self::NodeSocketTaskMetadata,
                        PeriodicNodeSocketTask = Self::PeriodicNodeSocketTask,
                        PeriodTimeUnit = Self::PeriodTimeUnit,
                        RouteStorage = Self::RouteStorage,
                        RouteId = Self::RouteId,
                        ConnectionInfo = Self::ConnectionInfo,
                        StreamType = Self::StreamType,
                    > + Send
                    + Sync,
            >,
        >,
    ) -> Result<(), String>;

    fn route_handler(&self) -> Arc<Self::RouteHandler>;

    fn add_socket_task(&self, id: Self::RouteId, task: Box<Self::RouteTask>) -> Result<(), String>;

    fn node_identifier(
        &self,
    ) -> Arc<dyn NodeIdentifier<Self::ConnectionInfo, Self::ConnectionValue> + Send + Sync>;

    fn membership(&self) -> Arc<RwLock<Self::Membership>>;

    fn init_neighbors(&self);

    fn data(&self) -> Arc<impl DataState + Send + Sync>;

    async fn init(&self) -> Result<(), NodeInitError>;
}

pub struct DefaultNodeState {
    sockets: DashMap<
        NodeAddress,
        Box<
            dyn NodeSocket<
                    RouteTask = DefaultNodeSocketTask,
                    NodeSocketTaskMetadata = DefaultNodeSocketTaskMetadata,
                    PeriodicNodeSocketTask = PeriodicDefaultNodeSocketTask,
                    PeriodTimeUnit = TokioPeriodTimeUnit,
                    RouteStorage = HashMapRouteStorage,
                    RouteId = NodeSocketRouteId,
                    ConnectionInfo = NodeAddress,
                    StreamType = Vec<u8>,
                > + Send
                + Sync,
        >,
    >,
    membership: Arc<RwLock<DefaultMembership>>,
    data: Arc<DefaultDataState>,
    config: Arc<
        dyn NodeConfig<
                DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>,
                DefaultMembershipNeighbor,
            > + Send
            + Sync,
    >,
    identifier: Arc<dyn NodeIdentifier<NodeAddress, NodeAddress> + Send + Sync>,
    route_handler: Arc<DefaultRouteHandler>,
}

#[async_trait]
impl NodeState for DefaultNodeState {
    type RouteTask = DefaultNodeSocketTask;
    type NodeSocketTaskMetadata = DefaultNodeSocketTaskMetadata;
    type Membership = DefaultMembership;
    type MembershipNeighbor = DefaultMembershipNeighbor;
    type MembershipNeighborRepresentation =
        DefaultMembershipNeighborRepresentation<Self::MembershipNeighbor>;
    type ConnectionValue = NodeAddress;
    type PeriodTimeUnit = TokioPeriodTimeUnit;
    type PeriodicNodeSocketTask = PeriodicDefaultNodeSocketTask;
    type RouteHandler = DefaultRouteHandler;
    type RouteStorage = HashMapRouteStorage;

    type RouteId = NodeSocketRouteId;
    type ConnectionInfo = NodeAddress;
    type StreamType = Vec<u8>;

    fn add_socket(
        &self,
        port: NodeAddress,
        socket: Box<
            dyn NodeSocket<
                    RouteTask = Self::RouteTask,
                    NodeSocketTaskMetadata = Self::NodeSocketTaskMetadata,
                    PeriodicNodeSocketTask = Self::PeriodicNodeSocketTask,
                    PeriodTimeUnit = Self::PeriodTimeUnit,
                    RouteStorage = Self::RouteStorage,
                    RouteId = Self::RouteId,
                    ConnectionInfo = Self::ConnectionInfo,
                    StreamType = Self::StreamType,
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

    fn node_identifier(&self) -> Arc<dyn NodeIdentifier<NodeAddress, NodeAddress> + Send + Sync> {
        self.identifier.clone()
    }

    fn membership(&self) -> Arc<RwLock<Self::Membership>> {
        self.membership.clone()
    }

    fn add_socket_task_and_create(
        &self,
        id: NodeSocketRouteId,
        task: Box<Self::RouteTask>,
        socket_constructor: Box<
            dyn Fn(
                NodeAddress,
            ) -> Box<
                dyn NodeSocket<
                        RouteTask = Self::RouteTask,
                        NodeSocketTaskMetadata = Self::NodeSocketTaskMetadata,
                        PeriodicNodeSocketTask = Self::PeriodicNodeSocketTask,
                        PeriodTimeUnit = Self::PeriodTimeUnit,
                        RouteStorage = Self::RouteStorage,
                        RouteId = Self::RouteId,
                        ConnectionInfo = Self::ConnectionInfo,
                        StreamType = Self::StreamType,
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
        port: NodeAddress,
        task: Arc<PeriodicDefaultNodeSocketTask>,
    ) -> Result<(), String> {
        match self.sockets.get_mut(&port) {
            Some(mut socket) => {
                socket.add_periodic_task(task).await;
                Ok(())
            }
            None => Err(format!("Socket with port {} not found", port.port())),
        }
    }

    fn add_socket_task(
        &self,
        id: NodeSocketRouteId,
        task: Box<Self::RouteTask>,
    ) -> Result<(), String> {
        self.route_handler()
            .add_route(id, Arc::new(NodeSocketRoute::new(task)));

        Ok(())
    }

    fn init_neighbors(&self) {
        let neighbors = self.config.neighbors().neighbors().read().unwrap().clone();

        for i in 0..neighbors.len() {
            let mut n = neighbors[i].write().unwrap();

            let neighbor_info = self.node_identifier().connection_info();

            if self.node_identifier().connection_info().port() == neighbor_info.port() {
                n.add_taint(Box::new(NodeAddressTaint::new(
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
            .collect::<Vec<NodeAddress>>();

        for key in keys {
            let socket = self.sockets.get_mut(&key);

            if let None = socket {
                return Err(NodeInitError::SocketDoesNotExist());
            }

            let listener = socket.unwrap().bind().await?;
        }

        Ok(())
    }

    fn data(&self) -> Arc<impl DataState + Send + Sync> {
        Arc::new(DefaultDataState {})
    }
}

impl DefaultNodeState {
    pub fn new(
        config: Arc<
            dyn NodeConfig<
                    DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>,
                    DefaultMembershipNeighbor,
                > + Send
                + Sync,
        >,
        identifier: Arc<dyn NodeIdentifier<NodeAddress, NodeAddress> + Send + Sync>,
        route_handler: Arc<DefaultRouteHandler>,
        data: Arc<DefaultDataState>,
    ) -> Self {
        Self {
            sockets: DashMap::new(),
            membership: Arc::new(RwLock::new(DefaultMembership::new())),
            config,
            data,
            identifier,
            route_handler,
        }
    }
}
