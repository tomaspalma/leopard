use crate::node::{NodeSocket, NodeSocketTaskMetadata, PeriodicNodeSocketTask, port::NodePort};
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
use std::net::TcpStream;

use std::io::Read;
use std::{net::TcpListener, sync::Arc};

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
    port: NodePort,
    listener: Option<TcpListener>,
    request_handler: Arc<dyn RequestHandler<TcpStream> + Send + Sync>,
    route_handler:
        Arc<dyn RouteHandler<HashMapRouteStorage, RouteId = NodeSocketRouteId> + Send + Sync>,
}

impl DefaultNodeSocket {
    pub fn new(port: NodePort) -> Self {
        Self {
            port,
            listener: None,
            request_handler: Arc::new(DefaultRequestHandler::new()),
            route_handler: Arc::new(DefaultRouteHandler::new()),
        }
    }
}

#[async_trait]
impl
    NodeSocket<
        DefaultNodeSocketTask,
        PeriodicDefaultNodeSocketTask,
        TokioPeriodTimeUnit,
        DefaultNodeSocketTaskMetadata,
        HashMapRouteStorage,
    > for DefaultNodeSocket
{
    type RouteId = NodeSocketRouteId;

    fn request_handler(&self) -> Arc<dyn RequestHandler<TcpStream>> {
        self.request_handler.clone()
    }

    fn route_handler(
        &self,
    ) -> Arc<dyn RouteHandler<HashMapRouteStorage, RouteId = NodeSocketRouteId> + Send + Sync> {
        self.route_handler.clone()
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
                        let msg = self.request_handler().handle(stream.bytes());

                        self.route_handler().handle(msg, self.port.clone()).await;
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
