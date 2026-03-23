use crate::storage::state::{DataState, DefaultDataState};

use dashmap::DashMap;

use tracing::{error, info};

use async_trait::async_trait;
use config::node::NodeConfig;
use connection::{node::default::NodeSocketRoute, route::RouteTask};
use errors::node::NodeInitError;
use message::Message;
use runtime::time::TokioPeriodTimeUnit;

use tokio::io::AsyncReadExt;

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use connection::route::{
    RouteHandler,
    default::{DefaultRouteHandler, HashMapRouteStorage, NodeSocketRouteId},
};

use connection::node::{
    NodeSocket,
    default::{
        DefaultNodeSocketTask, DefaultNodeSocketTaskMetadata, PeriodicDefaultNodeSocketTask,
    },
    id::NodeIdentifier,
    port::NodeAddress,
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

    fn register_storage(&self, key: String, storage: Arc<dyn DataState + Send + Sync>);

    fn get_storage(&self, key: String) -> Option<Arc<dyn DataState + Send + Sync>>;

    fn add_socket(
        &self,
        port: NodeAddress,
        socket: Arc<
            tokio::sync::Mutex<
                dyn NodeSocket<
                        RouteTask = Self::RouteTask,
                        NodeSocketTaskMetadata = Self::NodeSocketTaskMetadata,
                        PeriodicNodeSocketTask = Self::PeriodicNodeSocketTask,
                        PeriodTimeUnit = Self::PeriodTimeUnit,
                        RouteStorage = Self::RouteStorage,
                        RouteId = Self::RouteId,
                        ConnectionInfo = Self::ConnectionInfo,
                        StreamType = Self::StreamType,
                        RequestHandlerReturn = u64,
                    > + Send
                    + Sync,
            >,
        >,
    ) -> Result<(), String>;

    async fn send_through_socket(
        &self,
        port: NodeAddress,
        target: Box<Self::ConnectionInfo>,
        message: Box<dyn Message + Send + Sync>,
    ) -> Result<(), String>;

    async fn add_periodic_socket_task(
        &self,
        port: NodeAddress,
        task: Arc<Self::PeriodicNodeSocketTask>,
    ) -> Result<(), String>;
    fn add_socket_task_and_create(
        &self,
        id: Self::RouteId,
        task: Arc<dyn RouteTask + Send + Sync>,
        socket_constructor: Box<
            dyn Fn(
                NodeAddress,
            ) -> Arc<
                tokio::sync::Mutex<
                    dyn NodeSocket<
                            RouteTask = Self::RouteTask,
                            NodeSocketTaskMetadata = Self::NodeSocketTaskMetadata,
                            PeriodicNodeSocketTask = Self::PeriodicNodeSocketTask,
                            PeriodTimeUnit = Self::PeriodTimeUnit,
                            RouteStorage = Self::RouteStorage,
                            RouteId = Self::RouteId,
                            ConnectionInfo = Self::ConnectionInfo,
                            StreamType = Self::StreamType,
                            RequestHandlerReturn = u64,
                        > + Send
                        + Sync,
                >,
            >,
        >,
    ) -> Result<(), String>;

    fn route_handler(&self) -> Arc<Self::RouteHandler>;

    fn add_socket_task(
        &self,
        id: Self::RouteId,
        task: Arc<dyn RouteTask + Send + Sync>,
    ) -> Result<(), String>;

    fn node_identifier(
        &self,
    ) -> Arc<dyn NodeIdentifier<Self::ConnectionInfo, Self::ConnectionValue> + Send + Sync>;

    fn membership(&self) -> Arc<RwLock<Self::Membership>>;

    fn init_neighbors(&self);

    async fn init(&self) -> Result<(), NodeInitError>;
}

pub struct DefaultNodeState {
    sockets: Arc<
        tokio::sync::Mutex<
            HashMap<
                NodeAddress,
                Arc<
                    tokio::sync::Mutex<
                        dyn NodeSocket<
                                RouteTask = DefaultNodeSocketTask,
                                NodeSocketTaskMetadata = DefaultNodeSocketTaskMetadata,
                                PeriodicNodeSocketTask = PeriodicDefaultNodeSocketTask,
                                PeriodTimeUnit = TokioPeriodTimeUnit,
                                RouteStorage = HashMapRouteStorage,
                                RouteId = NodeSocketRouteId,
                                ConnectionInfo = NodeAddress,
                                StreamType = Vec<u8>,
                                RequestHandlerReturn = u64,
                            > + Send
                            + Sync,
                    >,
                >,
            >,
        >,
    >,
    membership: Arc<RwLock<DefaultMembership>>,
    data: DashMap<String, Arc<dyn DataState + Send + Sync>>,
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

    fn register_storage(&self, key: String, storage: Arc<dyn DataState + Send + Sync>) {
        self.data.insert(key, storage);
    }

    fn get_storage(&self, key: String) -> Option<Arc<dyn DataState + Send + Sync>> {
        Some(self.data.get(&key).unwrap().clone())
    }

    fn add_socket(
        &self,
        port: NodeAddress,
        socket: Arc<
            tokio::sync::Mutex<
                dyn NodeSocket<
                        RouteTask = Self::RouteTask,
                        NodeSocketTaskMetadata = Self::NodeSocketTaskMetadata,
                        PeriodicNodeSocketTask = Self::PeriodicNodeSocketTask,
                        PeriodTimeUnit = Self::PeriodTimeUnit,
                        RouteStorage = Self::RouteStorage,
                        RouteId = Self::RouteId,
                        ConnectionInfo = Self::ConnectionInfo,
                        StreamType = Self::StreamType,
                        RequestHandlerReturn = u64,
                    > + Send
                    + Sync,
            >,
        >,
    ) -> Result<(), String> {
        self.sockets
            .try_lock()
            .unwrap()
            .insert(port.clone(), socket);
        Ok(())
    }

    async fn send_through_socket(
        &self,
        port: NodeAddress,
        target: Box<Self::ConnectionInfo>,
        message: Box<dyn Message + Send + Sync>,
    ) -> Result<(), String> {
        let s = self.sockets.lock().await;
        let socket = s.get(&port).unwrap();

        socket.lock().await.send(target, message).await;

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
        task: Arc<dyn RouteTask + Send + Sync>,
        socket_constructor: Box<
            dyn Fn(
                NodeAddress,
            ) -> Arc<
                tokio::sync::Mutex<
                    dyn NodeSocket<
                            RouteTask = Self::RouteTask,
                            NodeSocketTaskMetadata = Self::NodeSocketTaskMetadata,
                            PeriodicNodeSocketTask = Self::PeriodicNodeSocketTask,
                            PeriodTimeUnit = Self::PeriodTimeUnit,
                            RouteStorage = Self::RouteStorage,
                            RouteId = Self::RouteId,
                            ConnectionInfo = Self::ConnectionInfo,
                            StreamType = Self::StreamType,
                            RequestHandlerReturn = u64,
                        > + Send
                        + Sync,
                >,
            >,
        >,
    ) -> Result<(), String> {
        let element_exists = self
            .sockets
            .try_lock()
            .unwrap()
            .contains_key(&id.info().port());

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
        match self.sockets.lock().await.get_mut(&port) {
            Some(mut socket) => {
                socket.lock().await.add_periodic_task(task).await;
                Ok(())
            }
            None => Err(format!("Socket with port {} not found", port.port())),
        }
    }

    fn add_socket_task(
        &self,
        id: NodeSocketRouteId,
        task: Arc<dyn RouteTask + Send + Sync>,
    ) -> Result<(), String> {
        self.route_handler()
            .add_route(id, Arc::new(NodeSocketRoute::new(task)));

        Ok(())
    }

    fn init_neighbors(&self) {
        let neighbors = self.config.neighbors().neighbors().read().unwrap().clone();

        for i in 0..neighbors.len() {
            let mut n = neighbors[i].write().unwrap();

            let neighbor_info = n.identifier().connection_info();

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
            .lock()
            .await
            .iter()
            .map(|x| x.0.clone())
            .collect::<Vec<NodeAddress>>();

        for key in keys {
            let socket_arc = {
                let mut guard = self.sockets.lock().await;
                guard.get(&key).cloned()
            };

            let socket = socket_arc.ok_or(NodeInitError::SocketDoesNotExist())?;

            let value = socket.clone();
            let route_handler_clone = self.route_handler.clone();
            runtime::spawn(async move {
                let socket_clone = value.clone();
                let route_handler = route_handler_clone.clone();

                {
                    socket_clone.lock().await.bind().await.unwrap();
                };

                let (listener, address, request_handler) = {
                    let mut guard = socket_clone.lock().await;
                    (
                        guard.listener().clone(),
                        guard.connection_info(),
                        guard.request_handler().clone(),
                    )
                };

                loop {
                    info!("Waiting for connection on port {}...", address.port());

                    match listener.accept().await {
                        Ok((mut stream, addr)) => {
                            info!("Accepted connection from {}", addr);

                            let mut buffer = Vec::new();

                            match stream.read_to_end(&mut buffer).await {
                                Ok(_) => {
                                    info!("Buffers length: {}", buffer.len());
                                    let protocol_id = request_handler.handle(buffer.clone());

                                    route_handler
                                        .handle(buffer, protocol_id, address.clone())
                                        .await;
                                }
                                Err(e) => {
                                    error!("Failed to read from stream: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to accept connection: {}", e);
                        }
                    }
                }
            });
        }

        Ok(())
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
    ) -> Self {
        Self {
            sockets: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            membership: Arc::new(RwLock::new(DefaultMembership::new())),
            config,
            data: DashMap::new(),
            identifier,
            route_handler,
        }
    }
}
