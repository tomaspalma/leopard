pub mod default;
pub mod id;
pub mod port;

use crate::node::port::NodePort;
use crate::request::handler::RequestHandler;
use crate::route::{RouteHandler, RouteStorage};

use async_trait::async_trait;
use message::{DefaultMessage, DefaultMessageType, MessageType};
use runtime::{Task, time::PeriodTimeUnit};

use std::net::TcpStream;

use std::sync::Arc;

pub trait NodeSocketTaskMetadata {}

#[async_trait]
pub trait NodeSocketTask<M: NodeSocketTaskMetadata> {
    async fn run(&self);
    fn metadata(&self) -> Arc<M>;
}

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
pub trait NodeSocket<T, PT, PTU, M, MType, RStorage>
where
    T: NodeSocketTask<M>,
    PT: PeriodicNodeSocketTask<PTU>,
    M: NodeSocketTaskMetadata,
    PTU: PeriodTimeUnit + Send + Sync,
    MType: MessageType,
    RStorage: RouteStorage,
{
    fn request_handler(
        &self,
    ) -> Arc<dyn RequestHandler<DefaultMessage, DefaultMessageType, TcpStream>>;
    fn route_handler(&self) -> Arc<dyn RouteHandler<MType, RStorage> + Send + Sync>;
    async fn add_periodic_task(&mut self, task: Arc<PT>);
    async fn bind(&mut self) -> Result<(), std::io::Error>;
    async fn send(&self);
    async fn receive(&self);
    async fn disconnect(&self);
}
