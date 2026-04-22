use crate::node::{
    NodeSocket, NodeSocketTaskMetadata, PeriodicNodeSocketTask, port::NodeAddress,
};
use crate::protocol::id_translator::ProtocolIdTranslator;
use crate::request::handler::{RequestHandler, default::DefaultRequestHandler};
use crate::route::{
    Route, RouteHandler, RouteTask,
    default::{DefaultRouteHandler, HashMapRouteStorage, NodeSocketRouteId},
};
use tracing::{error, info};

use async_trait::async_trait;
use message::Message;
use runtime::spawn;
use runtime::metrics::experiment::get_context;
use runtime::{
    Task,
    time::{PeriodTimeUnit, TokioPeriodTimeUnit},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use zeromq::Socket;

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
    fn run(self: Arc<Self>, message: Vec<u8>, neighbor: NodeAddress) {
        info!("Running task!");
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
    request_handler: Arc<dyn RequestHandler<Vec<u8>, u64> + Send + Sync>,
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
    type RequestHandlerReturn = u64;

    fn connection_info(&self) -> NodeAddress {
        self.port.clone()
    }

    fn request_handler(&self) -> Arc<dyn RequestHandler<Vec<u8>, u64> + Send + Sync> {
        self.request_handler.clone()
    }

    fn route_handler(&self) -> Arc<dyn RouteHandler<RouteId = NodeSocketRouteId> + Send + Sync> {
        self.route_handler.clone()
    }

    fn listener(&self) -> Arc<TcpListener> {
        self.listener.clone().expect("Listener not initialized")
    }

    async fn add_periodic_task(&mut self, task: Arc<PeriodicDefaultNodeSocketTask>) {
        spawn!({
            task.run().await;
        });
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
                let message_to_send = message
                    .serialize(message.protocol(), self.port.port())
                    .unwrap();

                match stream.write_all(&message_to_send).await {
                    Ok(_) => {
                        let bytes_sent = message_to_send.len() as u64;
                        let target_str = format!("{:?}", target);
                        let context = get_context();
                        let protocol_id = message.protocol().unwrap_or(0);
                        let protocol_label = ProtocolIdTranslator::translate(protocol_id);

                        metrics::counter!(
                            "total_bytes_sent",
                            "target" => target_str.clone(),
                            "protocol" => protocol_label,
                            "run_id" => context.run_id().to_string(),
                            "trial" => context.trial().to_string(),
                            "similarity" => context.similarity().to_string()
                        )
                        .increment(bytes_sent);

                        match protocol_id {
                            1 => {
                                metrics::counter!(
                                    "riblt_bytes_sent",
                                    "target" => target_str.clone(),
                                    "run_id" => context.run_id().to_string(),
                                    "trial" => context.trial().to_string(),
                                    "similarity" => context.similarity().to_string()
                                )
                                .increment(bytes_sent);
                                metrics::counter!(
                                    "protocol_bytes_sent",
                                    "target" => target_str,
                                    "protocol" => protocol_label,
                                    "run_id" => context.run_id().to_string(),
                                    "trial" => context.trial().to_string(),
                                    "similarity" => context.similarity().to_string()
                                )
                                .increment(bytes_sent);
                            }
                            2 => {
                                metrics::counter!(
                                    "merkle_bytes_sent",
                                    "target" => target_str.clone(),
                                    "run_id" => context.run_id().to_string(),
                                    "trial" => context.trial().to_string(),
                                    "similarity" => context.similarity().to_string()
                                )
                                .increment(bytes_sent);
                                metrics::counter!(
                                    "protocol_bytes_sent",
                                    "target" => target_str,
                                    "protocol" => protocol_label,
                                    "run_id" => context.run_id().to_string(),
                                    "trial" => context.trial().to_string(),
                                    "similarity" => context.similarity().to_string()
                                )
                                .increment(bytes_sent);
                            }
                            3 => {
                                metrics::counter!(
                                    "protocol_bytes_sent",
                                    "target" => target_str,
                                    "protocol" => protocol_label,
                                    "run_id" => context.run_id().to_string(),
                                    "trial" => context.trial().to_string(),
                                    "similarity" => context.similarity().to_string()
                                )
                                .increment(bytes_sent);
                            }
                            _ => {}
                        }

                        let _ = stream.flush().await;
                    }
                    Err(e) => {
                        error!("Failed to send data to {}: {}", addr, e);
                    }
                }
            }
            Err(e) => {
                error!("Could not connect to target {}: {}", addr, e);
            }
        }
    }

    async fn disconnect(&self) {}
}

pub struct NodeSocketRoute {
    task: Arc<dyn RouteTask + Send + Sync>,
}

impl NodeSocketRoute {
    pub fn new(task: Arc<dyn RouteTask + Send + Sync>) -> Self {
        Self { task }
    }
}

impl Route for NodeSocketRoute {
    fn task(&self) -> Arc<dyn RouteTask> {
        self.task.clone()
    }
}
