use crate::node::{NodeSocket, NodeSocketTaskMetadata, PeriodicNodeSocketTask, port::NodeAddress};
use crate::request::handler::{RequestHandler, default::DefaultRequestHandler};
use crate::route::{
    Route, RouteHandler, RouteTask,
    default::{DefaultRouteHandler, HashMapRouteStorage, NodeSocketRouteId},
};
use tracing::{error, info};

use async_trait::async_trait;
use message::{Message, ProtocolIDTranslator};
use runtime::metrics::{MetricRegistry, experiment::get_context};
use runtime::spawn;
use runtime::{
    Task,
    time::{PeriodTimeUnit, TokioPeriodTimeUnit},
};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use zeromq::Socket;

use std::collections::HashMap;
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
    fn run(self: Arc<Self>, _message: Vec<u8>, _neighbor: NodeAddress) {
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

type PooledConnection = Arc<Mutex<Option<TcpStream>>>;

pub struct DefaultNodeSocket {
    port: NodeAddress,
    listener: Option<Arc<TcpListener>>,
    request_handler: Arc<dyn RequestHandler<Vec<u8>, u64> + Send + Sync>,
    route_handler: Arc<dyn RouteHandler<RouteId = NodeSocketRouteId> + Send + Sync>,
    connections: Arc<Mutex<HashMap<String, PooledConnection>>>,
}

impl DefaultNodeSocket {
    pub fn new(port: NodeAddress) -> Self {
        Self {
            port,
            listener: None,
            request_handler: Arc::new(DefaultRequestHandler::new()),
            route_handler: Arc::new(DefaultRouteHandler::new()),
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn send_with_retries(
        &self,
        addr: &str,
        conn: &mut Option<TcpStream>,
        frame_len: [u8; 4],
        message: &[u8],
    ) -> bool {
        for attempt in 0..2 {
            if conn.is_none() {
                match TcpStream::connect(addr).await {
                    Ok(stream) => {
                        let _ = stream.set_nodelay(true);
                        *conn = Some(stream);
                    }
                    Err(e) => {
                        error!("Could not connect to target {}: {}", addr, e);
                        return false;
                    }
                }
            }

            let stream = conn.as_mut().unwrap();
            let write_result = async {
                stream.write_all(&frame_len).await?;
                stream.write_all(message).await?;
                stream.flush().await
            }
            .await;

            match write_result {
                Ok(_) => return true,
                Err(e) => {
                    *conn = None;
                    if attempt == 1 {
                        error!("Failed to send data to {}: {}", addr, e);
                    }
                }
            }
        }

        false
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
        let _socket = zeromq::RepSocket::new();

        let listener = TcpListener::bind(format!("127.0.0.1:{}", self.port.port())).await?;

        self.listener = Some(Arc::new(listener));

        Ok(())
    }

    async fn send(&self, target: Box<NodeAddress>, message: Box<dyn Message + Send + Sync>) {
        let addr = format!("{}:{}", target.host(), target.port());

        let message_to_send = match message.serialize(message.protocol(), self.port.port()) {
            Ok(bytes) => bytes,
            Err(_) => {
                error!("Failed to serialize message for {}", addr);
                return;
            }
        };
        let frame_len = message_to_send.len() as u32;

        let slot = {
            let mut pool = self.connections.lock().await;
            pool.entry(addr.clone())
                .or_insert_with(|| Arc::new(Mutex::new(None)))
                .clone()
        };
        let mut conn = slot.lock().await;

        if self
            .send_with_retries(&addr, &mut conn, frame_len.to_be_bytes(), &message_to_send)
            .await
        {
            let bytes_sent = message_to_send.len() as u64 + 4;
            let target_str = format!("{:?}", target);
            let context = get_context();
            let protocol_id = message.protocol().unwrap_or(0);
            let protocol_label = ProtocolIDTranslator::translate(protocol_id);

            MetricRegistry::record_counter_metric(
                protocol_id,
                bytes_sent,
                &target_str,
                &protocol_label,
                &context,
            );
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
