pub mod port;
pub mod iroh;

use async_trait::async_trait;
use crate::node::port::NodePort;

use std::sync::Arc;

pub trait NodeSocketTaskMetadata {}

pub trait NodeSocketTask<M: NodeSocketTaskMetadata> {
    fn run(&self);
    fn metadata(&self) -> Arc<M>;
}


#[async_trait]
pub trait NodeSocket<T: NodeSocketTask<M>, M: NodeSocketTaskMetadata> {
    fn add_task(&mut self, port: NodePort, task: Box<T>);
    async fn bind(&self);
    async fn send(&self);
    async fn receive(&self);
    async fn disconnect(&self);
}

