pub mod port;

use iroh::{Endpoint};
use async_trait::async_trait;
use crate::connection::port::NodePort;

pub trait NodeSocketTask {
    fn run(&self);
}

#[async_trait]
pub trait NodeSocket {
    fn add_task(&mut self, port: NodePort, task: Box<dyn NodeSocketTask + Send + Sync>);
    async fn bind(&self);
    async fn send(&self);
    async fn receive(&self);
    async fn disconnect(&self);
}

pub struct DefaultNodeSocket {
    port: NodePort,
    tasks: Vec<Box<dyn NodeSocketTask + Send + Sync>>
}

impl DefaultNodeSocket {
    pub fn new(port: NodePort) -> Self {
        Self {
            port,
            tasks: vec![]
        }
    }
}

#[async_trait]
impl NodeSocket for DefaultNodeSocket {
    fn add_task(&mut self, port: NodePort, task: Box<dyn NodeSocketTask + Send + Sync>) {
        println!("Adding task to socket");
        self.tasks.push(task);
    }

    async fn bind(&self) {
        println!("Binding socket");
        let endpoint = Endpoint::bind().await.unwrap(); 
    }

    async fn send(&self) {

    }

    async fn receive(&self) {

    }

    async fn disconnect(&self) {

    }
}
