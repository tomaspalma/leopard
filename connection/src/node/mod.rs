pub mod default;
pub mod id;
pub mod port;

use crate::node::port::NodePort;
use async_trait::async_trait;
use runtime::time::PeriodTimeUnit;

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
            self.run_task();

            self.interval().tick().await;
        }
    }
    fn run_task(&self);
    fn interval(&self) -> Arc<dyn PeriodTimeUnit + Send + Sync>;
}

#[async_trait]
pub trait NodeSocket<T, PT, PTU, M>
where
    T: NodeSocketTask<M>,
    PT: PeriodicNodeSocketTask<PTU>,
    M: NodeSocketTaskMetadata,
    PTU: PeriodTimeUnit + Send + Sync,
{
    fn add_task(&mut self, port: NodePort, task: Box<T>);
    async fn add_periodic_task(&mut self, port: NodePort, task: Arc<PT>, interval: Arc<PTU>);
    async fn bind(&mut self) -> Result<(), std::io::Error>;
    async fn send(&self);
    async fn receive(&self);
    async fn disconnect(&self);
}
