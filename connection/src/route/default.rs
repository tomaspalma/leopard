use crate::node::port::NodeAddress;
use crate::route::{Route, RouteHandler, RouteId, RouteStorage};

use async_trait::async_trait;
use dashmap::DashMap;
use runtime::RUNTIME;
use std::sync::Arc;

#[derive(Clone, Hash, Eq, PartialEq)]
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

#[derive(Hash, Eq, PartialEq)]
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

    fn store(&self, key: Self::Key, value: Self::Value) {
        self.storage.insert(key, value);
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

    async fn handle(&self, protocol: u64, port: NodeAddress) {
        let route = self
            .storage
            .get(NodeSocketRouteId::new(port.clone(), protocol));

        println!("Never found route");
        if let Some(route) = route {
            let rt_handle = {
                let guard = RUNTIME.read().unwrap();
                std::sync::Arc::clone(&*guard)
            };

            rt_handle
                .spawn(Box::new(move || {
                    Box::pin({
                        let value = route.clone();
                        // let message_clone = message.clone();
                        async move {
                            // value.task().run(message_clone);
                            Ok(())
                        }
                    })
                }))
                .await;
        } else {
            println!("No route found for port: {}", port.port());
        }

        println!("Handling route");
    }

    fn add_route(&self, id: NodeSocketRouteId, route: Arc<dyn Route + Send + Sync>) {
        self.storage.store(id, route);

        println!("Current routes stored: {}", self.storage.storage.len());
    }
}
