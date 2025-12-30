use async_trait::async_trait;

use std::collections::HashMap;
use crate::connection::{NodeSocket, DefaultNodeSocket, port::NodePort, NodeSocketTask};

#[async_trait]
pub trait NodeState {
    fn add_socket(&mut self, port: NodePort, socket: Box<dyn NodeSocket + Send + Sync>) -> Result<(), String>;
    fn add_socket_task(&mut self, port: NodePort, task: Box<dyn NodeSocketTask + Send + Sync>) -> Result<(), String>;
    fn add_socket_task_and_create(&mut self, port: NodePort, task: Box<dyn NodeSocketTask + Send + Sync>);
    async fn init(&mut self);
}

pub struct DefaultNodeState {
    sockets: HashMap<NodePort, Box<dyn NodeSocket + Send + Sync>>
}

#[async_trait]
impl NodeState for DefaultNodeState {
    fn add_socket(&mut self, port: NodePort, socket: Box<dyn NodeSocket + Send + Sync>) -> Result<(), String> {
        match self.sockets.insert(port.clone(), socket) {
            Some(_) => Ok(()),
            _ => Err(format!("Socket with port {} already exists", port.value()))
        }
    }

    fn add_socket_task_and_create(&mut self, port: NodePort, task: Box<dyn NodeSocketTask + Send + Sync>) {
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

    fn add_socket_task(&mut self, port: NodePort, task: Box<dyn NodeSocketTask + Send + Sync>) -> Result<(), String> {
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

    async fn init(&mut self) {
        for (_, socket) in self.sockets.iter_mut() {
            socket.bind().await;
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

