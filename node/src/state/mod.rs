use std::collections::HashMap;
use crate::connection::{NodeSocket, DefaultNodeSocket, port::NodePort, NodeSocketTask};

pub trait NodeState {
    fn add_socket(&mut self, port: NodePort, socket: Box<dyn NodeSocket>) -> Result<(), String>;
    fn add_socket_task(&mut self, port: NodePort, task: Box<dyn NodeSocketTask>) -> Result<(), String>;
    fn add_socket_task_and_create(&mut self, port: NodePort, task: Box<dyn NodeSocketTask>);
}

pub struct DefaultNodeState {
    sockets: HashMap<NodePort, Box<dyn NodeSocket>>
}

impl NodeState for DefaultNodeState {
    fn add_socket(&mut self, port: NodePort, socket: Box<dyn NodeSocket>) -> Result<(), String> {
        match self.sockets.insert(port.clone(), socket) {
            Some(_) => Ok(()),
            _ => Err(format!("Socket with port {} already exists", port.value()))
        }
    }

    fn add_socket_task_and_create(&mut self, port: NodePort, task: Box<dyn NodeSocketTask>) {
        match self.sockets.get_mut(&port) {
            Some(socket) => {
                socket.add_task(port, task);
            },
            None => {
                let socket = DefaultNodeSocket::new(port.clone());
                self.add_socket(port.clone(), Box::new(socket));
                self.add_socket_task(port, task);
            }
        }
    }

    fn add_socket_task(&mut self, port: NodePort, task: Box<dyn NodeSocketTask>) -> Result<(), String> {
        match self.sockets.get_mut(&port) {
            Some(socket) => {
                socket.add_task(port, task);
                Ok(())
            },
            None => {
                Err(format!("Socket with port {} not found", port.value()))
            }
        }
    }
}

impl DefaultNodeState {
    pub fn new() -> Self {
        Self {
            sockets: HashMap::new()
        }
    }
}

