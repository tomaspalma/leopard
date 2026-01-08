use async_trait::async_trait;
use crate::node::{port::NodePort, NodeSocketTask, NodeSocket, NodeSocketTaskMetadata };

use std::sync::Arc;

use iroh::{Endpoint};

pub struct DefaultNodeSocketTask {
    metadata: Arc<dyn NodeSocketTaskMetadata + Send + Sync>
}

#[derive(Clone)]
pub struct DefaultNodeSocketTaskMetadata {
    protocol: String
}

impl NodeSocketTaskMetadata for DefaultNodeSocketTaskMetadata {}

impl NodeSocketTask for DefaultNodeSocketTask {
    fn run(&self) {
        println!("Running task");
    }
}

pub struct DefaultNodeSocket<T> {
    port: NodePort,
    tasks: Vec<Box<T>>
}

impl DefaultNodeSocket<DefaultNodeSocketTask> {
    pub fn new(port: NodePort) -> Self {
        Self {
            port,
            tasks: vec![]
        }
    }
}

#[async_trait]
impl NodeSocket<DefaultNodeSocketTask> for DefaultNodeSocket<DefaultNodeSocketTask> {
    fn add_task(&mut self, port: NodePort, task: Box<DefaultNodeSocketTask>) {
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
