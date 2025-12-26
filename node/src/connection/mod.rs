pub mod port;

use crate::connection::port::NodePort;

pub trait NodeSocketTask {
    fn run(&self);
}

pub trait NodeSocket {
    fn add_task(&mut self, port: NodePort, task: Box<dyn NodeSocketTask>);

}

pub struct DefaultNodeSocket {
    port: NodePort,
    tasks: Vec<Box<dyn NodeSocketTask>>
}

impl DefaultNodeSocket {
    pub fn new(port: NodePort) -> Self {
        Self {
            port,
            tasks: vec![]
        }
    }
}

impl NodeSocket for DefaultNodeSocket {
    fn add_task(&mut self, port: NodePort, task: Box<dyn NodeSocketTask>) {
        self.tasks.push(task);
    }
}
