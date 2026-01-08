pub mod port;
pub mod iroh;

use async_trait::async_trait;
use crate::node::port::NodePort;

use std::sync::Arc;

pub trait NodeSocketTask {
    fn run(&self);
    // fn metadata(&self) -> Arc<dyn NodeSocketTaskMetadata + Send + Sync>;
}

pub trait NodeSocketTaskMetadata {}

#[async_trait]
pub trait NodeSocket<T> {
    fn add_task(&mut self, port: NodePort, task: Box<T>);
    async fn bind(&self);
    async fn send(&self);
    async fn receive(&self);
    async fn disconnect(&self);
}

