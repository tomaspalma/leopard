use async_trait::async_trait;
use dashmap::DashMap;
use message::{Message, MessageType};
use runtime::RUNTIME;
use std::sync::Arc;

use crate::node::port::NodePort;

pub trait RouteId<V> {
    fn id(&self) -> V;
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NodeSocketRouteIdInfo {
    port: NodePort,
    protocol: String,
}

impl NodeSocketRouteIdInfo {
    pub fn new(port: NodePort, protocol: String) -> Self {
        Self { port, protocol }
    }

    pub fn port(&self) -> NodePort {
        self.port.clone()
    }

    pub fn protocol(&self) -> String {
        self.protocol.clone()
    }
}

#[derive(Hash, Eq, PartialEq)]
pub struct NodeSocketRouteId {
    info: NodeSocketRouteIdInfo,
}

impl NodeSocketRouteId {
    pub fn new(port: NodePort, protocol: String) -> Self {
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

pub trait Route {
    fn task(&self) -> Box<dyn RouteTask>;
}

pub trait RouteTask {
    fn run(&self, message: Arc<dyn Message + Send + Sync>);
}

pub trait RouterHandlerInfo {
    type MType: MessageType;
    type RStorage: RouteStorage;
}

pub trait RouteStorage {
    type RouteIdValue;
    type Key: RouteId<Self::RouteIdValue>;
    type Value;

    fn store(&self, key: Self::Key, value: Self::Value);
    fn get(&self, id: Self::Key) -> Option<Self::Value>;
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

#[async_trait]
pub trait RouteHandler<RStorage>
where
    RStorage: RouteStorage,
{
    type RouteId;

    async fn handle(&self, message: Arc<dyn Message + Send + Sync>, port: NodePort);
    fn add_route(&self, id: Self::RouteId, route: Arc<dyn Route + Send + Sync>);
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
impl RouteHandler<HashMapRouteStorage> for DefaultRouteHandler {
    type RouteId = NodeSocketRouteId;

    async fn handle(&self, message: Arc<dyn Message + Send + Sync>, port: NodePort) {
        let route = self
            .storage
            .get(NodeSocketRouteId::new(port.clone(), String::new()));

        if let Some(route) = route {
            let rt_handle = {
                let guard = RUNTIME.read().unwrap();
                std::sync::Arc::clone(&*guard)
            };

            rt_handle
                .spawn(Box::new(move || {
                    Box::pin({
                        let value = route.clone();
                        let message_clone = message.clone();
                        async move {
                            value.task().run(message_clone);
                            Ok(())
                        }
                    })
                }))
                .await;
        } else {
            println!("No route found for port: {}", port.value());
        }

        println!("Handling route");
    }

    fn add_route(&self, id: NodeSocketRouteId, route: Arc<dyn Route + Send + Sync>) {
        self.storage.store(id, route);

        println!("Current routes stored: {}", self.storage.storage.len());
    }
}
