pub mod port;
pub mod iroh;

use async_trait::async_trait;
use crate::connection::port::NodePort;

pub trait NodeSocketTask {
    fn run(&self);
    // fn metadata(&self) -> Box<dyn NodeSocketTaskMetadata + Send + Sync>;
}

pub trait NodeSocketTaskMetadata {}

#[async_trait]
pub trait NodeSocket {
    fn add_task(&mut self, port: NodePort, task: Box<dyn NodeSocketTask + Send + Sync>);
    async fn bind(&self);
    async fn send(&self);
    async fn receive(&self);
    async fn disconnect(&self);
}

