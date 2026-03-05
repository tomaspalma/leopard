pub mod default;
pub mod id;
pub mod port;

use crate::request::handler::RequestHandler;
use crate::route::RouteHandler;

use async_trait::async_trait;
use message::Message;
use runtime::{Task, time::PeriodTimeUnit};

use std::sync::Arc;

pub trait NodeSocketTaskMetadata {}

#[async_trait]
pub trait PeriodicNodeSocketTask<I>
where
    I: PeriodTimeUnit + Send + Sync,
{
    async fn run(&self) {
        loop {
            self.run_task().await;

            self.interval().tick().await;
        }
    }
    fn task(&self) -> Arc<Task>;
    async fn run_task(&self);
    fn interval(&self) -> Arc<dyn PeriodTimeUnit + Send + Sync>;
}

#[async_trait]
pub trait NodeSocket {
    type RouteTask;
    type PeriodicNodeSocketTask;
    type NodeSocketTaskMetadata;
    type PeriodTimeUnit;
    type RouteStorage;

    type RouteId;
    type ConnectionInfo;
    type StreamType;

    fn request_handler(&self) -> Arc<dyn RequestHandler<Self::StreamType>>;
    fn route_handler(&self) -> Arc<dyn RouteHandler<RouteId = Self::RouteId> + Send + Sync>;
    async fn add_periodic_task(&mut self, task: Arc<Self::PeriodicNodeSocketTask>);
    async fn bind(&mut self) -> Result<(), std::io::Error>;
    async fn send(
        &self,
        target: Box<Self::ConnectionInfo>,
        message: Box<dyn Message + Send + Sync>,
    );
    async fn receive(&self);
    async fn disconnect(&self);
}
