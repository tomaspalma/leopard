pub mod port;

use crate::connection::port::NodePort;

pub trait NodeSocketTask {
    fn run(&self);
}

pub trait NodeSocket {
    fn add_task(&mut self, port: NodePort, task: Box<dyn NodeSocketTask>);
    fn bind(&self);
    fn send(&self);
    fn receive(&self);
    fn disconnect(&self);
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
        println!("Adding task to socket");
        self.tasks.push(task);
    }

    fn bind(&self) {

    }

    fn send(&self) {

    }

    fn receive(&self) {

    }

    fn disconnect(&self) {

    }
}
