pub mod default;

use async_trait::async_trait;
use dashmap::DashMap;
use message::{Message, MessageType};
use std::sync::Arc;

use crate::node::port::NodeAddress;

pub trait RouteId<V> {
    fn id(&self) -> V;
}

pub trait Route {
    fn task(&self) -> Arc<dyn RouteTask>;
}

pub trait RouteTask {
    fn run(self: Arc<Self>, message: Vec<u8>, neighbor: NodeAddress);
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

#[async_trait]
pub trait RouteHandler {
    type RouteId;

    async fn handle(
        &self,
        message: Vec<u8>,
        protocol: u64,
        local_address: NodeAddress,
        sender_address: NodeAddress,
    );
    fn add_route(&self, id: Self::RouteId, route: Arc<dyn Route + Send + Sync>);
}
