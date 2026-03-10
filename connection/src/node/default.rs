use crate::node::{NodeSocket, NodeSocketTaskMetadata, PeriodicNodeSocketTask, port::NodeAddress};
use crate::request::handler::{RequestHandler, default::DefaultRequestHandler};
use crate::route::{
    DefaultRouteHandler, HashMapRouteStorage, NodeSocketRouteId, Route, RouteHandler, RouteTask,
};

use async_trait::async_trait;
use message::Message;
use runtime::RUNTIME;
use runtime::{
    Runtime, Task,
    time::{PeriodTimeUnit, TokioPeriodTimeUnit},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use zeromq::Socket;

use std::io::{Bytes, Read};
use std::sync::Arc;

pub struct PeriodicDefaultNodeSocketTask {
    metadata: Arc<DefaultNodeSocketTaskMetadata>,
    task: Arc<Task>,
    interval: Arc<dyn PeriodTimeUnit + Send + Sync>,
}

impl PeriodicDefaultNodeSocketTask {
    pub fn new(
        metadata: Arc<DefaultNodeSocketTaskMetadata>,
        task: Arc<Task>,
        interval: Arc<dyn PeriodTimeUnit + Send + Sync>,
    ) -> Self {
        Self {
            metadata,
            task,
            interval,
        }
    }
}

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

impl RouteTask for DefaultNodeSocketTask {
    fn run(&self, message: Arc<dyn Message + Send + Sync>) {
        println!("Running task");
    }
}

#[async_trait]
impl PeriodicNodeSocketTask<TokioPeriodTimeUnit> for PeriodicDefaultNodeSocketTask {
    async fn run(&self) {
        loop {
            self.interval().tick().await;

            self.run_task().await;
        }
    }

    fn task(&self) -> Arc<Task> {
        self.task.clone()
    }

    async fn run_task(&self) {
        (self.task())().await.unwrap();
    }

    fn interval(&self) -> Arc<dyn PeriodTimeUnit + Send + Sync> {
        self.interval.clone()
    }
}

pub struct DefaultNodeSocket {
    port: NodeAddress,
    listener: Option<Arc<TcpListener>>,
    request_handler: Arc<dyn RequestHandler<Vec<u8>> + Send + Sync>,
    route_handler: Arc<dyn RouteHandler<RouteId = NodeSocketRouteId> + Send + Sync>,
}

impl DefaultNodeSocket {
    pub fn new(port: NodeAddress) -> Self {
        Self {
            port,
            listener: None,
            request_handler: Arc::new(DefaultRequestHandler::new()),
            route_handler: Arc::new(DefaultRouteHandler::new()),
        }
    }
}

#[async_trait]
impl NodeSocket for DefaultNodeSocket {
    type RouteTask = DefaultNodeSocketTask;
    type PeriodicNodeSocketTask = PeriodicDefaultNodeSocketTask;
    type NodeSocketTaskMetadata = DefaultNodeSocketTaskMetadata;
    type PeriodTimeUnit = TokioPeriodTimeUnit;
    type RouteStorage = HashMapRouteStorage;

    type RouteId = NodeSocketRouteId;
    type ConnectionInfo = NodeAddress;
    type StreamType = Vec<u8>;

    fn connection_info(&self) -> NodeAddress {
        self.port.clone()
    }

    fn request_handler(&self) -> Arc<dyn RequestHandler<Vec<u8>> + Send + Sync> {
        self.request_handler.clone()
    }

    fn route_handler(&self) -> Arc<dyn RouteHandler<RouteId = NodeSocketRouteId> + Send + Sync> {
        self.route_handler.clone()
    }

    fn listener(&self) -> Arc<TcpListener> {
        self.listener.clone().expect("Listener not initialized")
    }

    async fn add_periodic_task(&mut self, task: Arc<PeriodicDefaultNodeSocketTask>) {
        let rt_handle = {
            let rt_guard = RUNTIME.read().unwrap();
            Arc::clone(&*rt_guard)
        };

        rt_handle
            .spawn(Box::new(move || {
                Box::pin({
                    let value = task.clone();
                    async move {
                        value.run().await;
                        Ok(())
                    }
                })
            }))
            .await;
    }

    async fn bind(&mut self) -> Result<(), std::io::Error> {
        let mut socket = zeromq::RepSocket::new();

        let listener = TcpListener::bind(format!("127.0.0.1:{}", self.port.port())).await?;

        self.listener = Some(Arc::new(listener));

        Ok(())
    }

    async fn send(&self, target: Box<NodeAddress>, message: Box<dyn Message + Send + Sync>) {
        let addr = format!("{}:{}", target.host(), target.port());

        match TcpStream::connect(&addr).await {
            Ok(mut stream) => {
                let message_to_send = message.serialize().unwrap();

                match stream.write_all(&message_to_send).await {
                    Ok(_) => {
                        let _ = stream.flush().await;
                    }
                    Err(e) => {
                        eprintln!("Failed to send data to {}: {}", addr, e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Could not connect to target {}: {}", addr, e);
            }
        }
    }

    async fn disconnect(&self) {}
}

pub struct NodeSocketRoute {
    task: Box<dyn RouteTask + Send + Sync>,
}

impl NodeSocketRoute {
    pub fn new(task: Box<dyn RouteTask + Send + Sync>) -> Self {
        Self { task }
    }
}

impl Route for NodeSocketRoute {
    fn task(&self) -> Box<dyn RouteTask> {
        Box::new(DefaultNodeSocketTask::new(Arc::new(
            DefaultNodeSocketTaskMetadata::new(String::new()),
        )))
    }
}
