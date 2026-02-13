use crate::node::{NodeSocket, NodeSocketTask, NodeSocketTaskMetadata, port::NodePort};
use crate::request::handler::{RequestHandler, default::DefaultRequestHandler};
use async_trait::async_trait;
use runtime::time::PeriodTimeUnit;

use std::io::Read;
use std::{net::TcpListener, sync::Arc};

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

#[async_trait]
impl NodeSocketTask<DefaultNodeSocketTaskMetadata> for DefaultNodeSocketTask {
    async fn run(&self) {
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
    request_handler: Box<dyn RequestHandler + Send + Sync>,
}

impl DefaultNodeSocket<DefaultNodeSocketTask> {
    pub fn new(port: NodePort) -> Self {
        Self {
            port,
            tasks: vec![],
            listener: None,
            request_handler: Box::new(DefaultRequestHandler::new()),
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

    fn add_periodic_task(
        &mut self,
        port: NodePort,
        task: Box<DefaultNodeSocketTask>,
        interval: Arc<dyn PeriodTimeUnit>,
    ) {
    }

    async fn bind(&mut self) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", self.port.value()))?;

        self.listener = Some(listener);

        self.receive().await;

        Ok(())
    }

    async fn receive(&self) {
        if let Some(listener) = &self.listener {
            loop {
                println!("Waiting for connection");
                match listener.accept() {
                    Ok((stream, addr)) => {
                        println!("enfim");
                        println!("{:?}", stream.bytes());
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
