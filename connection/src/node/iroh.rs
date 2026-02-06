use crate::node::{NodeSocket, NodeSocketTask, NodeSocketTaskMetadata, port::NodePort};
use async_trait::async_trait;

use std::sync::Arc;

use iroh::{Endpoint, protocol::Router};

pub struct DefaultNodeSocketTask {
    metadata: Arc<DefaultNodeSocketTaskMetadata>,
}

impl DefaultNodeSocketTask {
    pub fn new(metadata: Arc<DefaultNodeSocketTaskMetadata>) -> Self {
        Self { metadata }
    }
}

#[derive(Clone)]
pub struct DefaultNodeSocketTaskMetadata {
    protocol: String,
}

impl NodeSocketTaskMetadata for DefaultNodeSocketTaskMetadata {}

impl DefaultNodeSocketTaskMetadata {
    pub fn new(protocol: String) -> Self {
        Self { protocol }
    }
}

impl NodeSocketTask<DefaultNodeSocketTaskMetadata> for DefaultNodeSocketTask {
    fn run(&self) {
        println!("Running task");
    }

    fn metadata(&self) -> Arc<DefaultNodeSocketTaskMetadata> {
        self.metadata.clone()
    }
}

pub struct DefaultNodeSocket<T> {
    port: NodePort,
    tasks: Vec<Box<T>>,
}

impl DefaultNodeSocket<DefaultNodeSocketTask> {
    pub fn new(port: NodePort) -> Self {
        Self {
            port,
            tasks: vec![],
        }
    }
}

#[async_trait]
impl NodeSocket<DefaultNodeSocketTask, DefaultNodeSocketTaskMetadata>
    for DefaultNodeSocket<DefaultNodeSocketTask>
{
    fn add_task(&mut self, port: NodePort, task: Box<DefaultNodeSocketTask>) {
        self.tasks.push(task);
    }

    async fn bind(&self) {
        let endpoint = Endpoint::bind().await.unwrap();

        let router = Router::builder(endpoint).spawn();
    }

    async fn send(&self) {}

    async fn receive(&self) {}

    async fn disconnect(&self) {}
}
