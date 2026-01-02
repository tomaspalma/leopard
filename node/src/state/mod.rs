use async_trait::async_trait;
use dashmap::{DashMap, mapref::multiple::{RefMulti, RefMutMulti}};

use crate::connection::{NodeSocket, DefaultNodeSocket, port::NodePort, NodeSocketTask};

#[async_trait]
pub trait NodeState {
    fn add_socket(&self, port: NodePort, socket: Box<dyn NodeSocket + Send + Sync>) -> Result<(), String>;
    fn add_socket_task(&self, port: NodePort, task: Box<dyn NodeSocketTask + Send + Sync>) -> Result<(), String>;
    fn add_socket_task_and_create(&self, port: NodePort, task: Box<dyn NodeSocketTask + Send + Sync>);

    async fn init(&self);
}

pub struct DefaultNodeState {
    sockets: DashMap<NodePort, Box<dyn NodeSocket + Send + Sync>>
}

#[async_trait]
impl NodeState for DefaultNodeState {
    fn add_socket(&self, port: NodePort, socket: Box<dyn NodeSocket + Send + Sync>) -> Result<(), String> {
        match self.sockets.insert(port.clone(), socket) {
            Some(_) => Ok(()),
            _ => Err(format!("Socket with port {} already exists", port.value()))
        }
    }

    fn add_socket_task_and_create(&self, port: NodePort, task: Box<dyn NodeSocketTask + Send + Sync>) {
        let element_exists = self.sockets.contains_key(&port);

        if !element_exists {
            self.add_socket(port.clone(), Box::new(DefaultNodeSocket::new(port.clone())));
        }

        self.add_socket_task(port, task);
    }

    fn add_socket_task(&self, port: NodePort, task: Box<dyn NodeSocketTask + Send + Sync>) -> Result<(), String> {
        match self.sockets.get_mut(&port) {
            Some(mut socket) => {
                socket.add_task(port, task);
                Ok(())
            },
            None => {
                Err(format!("Socket with port {} not found", port.value()))
            }
        }
    }

    async fn init(&self) {
        let keys = self.sockets.iter().map(|x| x.key().clone()).collect::<Vec<NodePort>>();
        
        for key in keys {
            let socket = self.sockets.get_mut(&key).unwrap();

            socket.bind().await;
        }
    }
}

impl DefaultNodeState {
    pub fn new() -> Self {
        Self {
            sockets: DashMap::new()
        }
    }
}

