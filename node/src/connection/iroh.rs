use async_trait::async_trait;
use crate::connection::{port::NodePort, NodeSocketTask, NodeSocket};

use iroh::{Endpoint, protocol::ProtocolHandler};

pub struct DefaultNodeSocketTask {
    protocol: String
}

impl NodeSocketTask for DefaultNodeSocketTask {
    fn run(&self) {
        println!("Running task");
    }
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
        self.tasks.push(task);
    }

    async fn bind(&self) {
        let endpoint = Endpoint::bind().await.unwrap(); 
    }

    async fn send(&self) {

    }

    async fn receive(&self) {

    }

    async fn disconnect(&self) {

    }
}
