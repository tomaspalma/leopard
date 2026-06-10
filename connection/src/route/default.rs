use crate::node::port::NodeAddress;
use crate::route::{Route, RouteHandler, RouteId, RouteStorage};
use tracing::info;

use async_trait::async_trait;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use runtime::spawn;
use std::sync::Arc;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct NodeSocketRouteIdInfo {
    port: NodeAddress,
    protocol: u64,
}

impl NodeSocketRouteIdInfo {
    pub fn new(port: NodeAddress, protocol: u64) -> Self {
        Self { port, protocol }
    }

    pub fn port(&self) -> NodeAddress {
        self.port.clone()
    }

    pub fn protocol(&self) -> u64 {
        self.protocol.clone()
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct NodeSocketRouteId {
    info: NodeSocketRouteIdInfo,
}

impl NodeSocketRouteId {
    pub fn new(port: NodeAddress, protocol: u64) -> Self {
        Self {
            info: NodeSocketRouteIdInfo { port, protocol },
        }
    }

    pub fn info(&self) -> NodeSocketRouteIdInfo {
        self.info.clone()
    }
}

impl RouteId<NodeSocketRouteIdInfo> for NodeSocketRouteId {
    fn id(&self) -> NodeSocketRouteIdInfo {
        self.info.clone()
    }
}

pub struct HashMapRouteStorage {
    storage: DashMap<NodeSocketRouteId, Arc<dyn Route + Send + Sync>>,
}

impl HashMapRouteStorage {
    pub fn new() -> Self {
        Self {
            storage: DashMap::new(),
        }
    }
}

impl RouteStorage for HashMapRouteStorage {
    type RouteIdValue = NodeSocketRouteIdInfo;
    type Key = NodeSocketRouteId;
    type Value = Arc<dyn Route + Send + Sync>;

    fn store(&self, key: Self::Key, value: Self::Value) -> Result<(), String> {
        match self.storage.entry(key) {
            Entry::Occupied(entry) => Err(format!(
                "route already registered for {:?}; each (port, protocol id) pair must be unique",
                entry.key()
            )),
            Entry::Vacant(entry) => {
                entry.insert(value);
                Ok(())
            }
        }
    }

    fn get(&self, id: Self::Key) -> Option<Self::Value> {
        self.storage.get(&id).map(|entry| entry.value().clone())
    }
}

pub struct DefaultRouteHandler {
    storage: HashMapRouteStorage,
}

impl DefaultRouteHandler {
    pub fn new() -> Self {
        Self {
            storage: HashMapRouteStorage::new(),
        }
    }
}

#[async_trait]
impl RouteHandler for DefaultRouteHandler {
    type RouteId = NodeSocketRouteId;

    async fn handle(
        &self,
        request: Vec<u8>,
        protocol: u64,
        local_address: NodeAddress,
        sender_address: NodeAddress,
    ) {
        let route = self
            .storage
            .get(NodeSocketRouteId::new(local_address.clone(), protocol));

        if let Some(route) = route {
            let request_clone = request.clone();
            spawn!({
                route.task().run(request_clone, sender_address.clone());
            });
        } else {
            info!("No route found for port: {}", local_address.port());
        }
    }

    fn add_route(
        &self,
        id: NodeSocketRouteId,
        route: Arc<dyn Route + Send + Sync>,
    ) -> Result<(), String> {
        self.storage.store(id, route)
    }
}
