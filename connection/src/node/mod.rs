pub mod default;
pub mod port;

use crate::node::port::NodePort;
use async_trait::async_trait;
use runtime::time::PeriodTimeUnit;

use std::sync::Arc;

pub trait NodeSocketTaskMetadata {}

pub trait NodeSocketTask<M: NodeSocketTaskMetadata> {
    fn run(&self);
    fn metadata(&self) -> Arc<M>;
}

pub trait PeriodicNodeSocketTask<I>
where
    I: PeriodTimeUnit,
{
    fn run(&self) {
        loop {
            self.run_task();

            self.interval().tick();
        }
    }
    fn run_task(&self);
    fn interval(&self) -> Arc<dyn PeriodTimeUnit>;
}

#[async_trait]
pub trait NodeSocket<T: NodeSocketTask<M>, M: NodeSocketTaskMetadata> {
    fn add_task(&mut self, port: NodePort, task: Box<T>);
    async fn bind(&mut self) -> Result<(), std::io::Error>;
    async fn send(&self);
    async fn receive(&self);
    async fn disconnect(&self);
}
