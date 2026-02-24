use dashmap::DashMap;
use message::{DefaultMessageType, Message, MessageType};
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

#[derive(Hash, Eq, PartialEq)]
pub struct NodeSocketRouteId {
    info: NodeSocketRouteIdInfo,
}

impl RouteId<NodeSocketRouteIdInfo> for NodeSocketRouteId {
    fn id(&self) -> NodeSocketRouteIdInfo {
        self.info.clone()
    }
}

pub trait Route {
    fn task(&self) -> Box<dyn RouteTask>;
}

pub trait RouteTask {}

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

pub trait RouteHandler<MType, RStorage>
where
    MType: MessageType,
    RStorage: RouteStorage,
{
    fn handle(&self, message: Box<dyn Message<MType>>);
    fn add_route(&self, id: String, route: Box<dyn Route>);
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

impl RouteHandler<DefaultMessageType, HashMapRouteStorage> for DefaultRouteHandler {
    fn handle(&self, message: Box<dyn Message<DefaultMessageType>>) {
        println!("Handling route");
    }

    fn add_route(&self, id: String, route: Box<dyn Route>) {}
}
