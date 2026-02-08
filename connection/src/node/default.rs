use crate::node::{NodeSocket, NodeSocketTask, NodeSocketTaskMetadata, port::NodePort};
use async_trait::async_trait;

use std::{net::TcpListener, sync::Arc};

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
    listener: Option<TcpListener>,
}

impl DefaultNodeSocket<DefaultNodeSocketTask> {
    pub fn new(port: NodePort) -> Self {
        Self {
            port,
            tasks: vec![],
            listener: None,
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

    async fn bind(&mut self) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", self.port.value()))?;

        self.listener = Some(listener);

        Ok(())
    }

    async fn receive(&self) {
        if let Some(listener) = &self.listener {
            loop {
                println!("Waiting for connection");
                match listener.accept() {
                    Ok((stream, addr)) => {
                        println!("New connection from {}", addr);
                    }
                    Err(e) => {
                        eprintln!("Failed to accept connection: {}", e);
                    }
                }
            }
        }
    }

    async fn send(&self) {}

    async fn disconnect(&self) {}
}
