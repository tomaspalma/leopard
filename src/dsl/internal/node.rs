use std::sync::Arc;
use std::{future::Future, pin::Pin};

use config::node::DefaultNodeConfig;
use connection::node::id::DefaultNodeIdentifier;
use connection::node::port::NodeAddress;
use connection::route::default::DefaultRouteHandler;
use runtime::Task;
use state::node::{DefaultNodeState, NodeState};
use tracing::info;

struct InternalNode {
    host: String,
    port: Option<u16>,
}

impl InternalNode {
    fn new() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: None,
        }
    }
}

pub struct NodeBuilder {
    nodes: Vec<InternalNode>,
}

impl NodeBuilder {
    fn create_empty_node_task(host: String, port: u16) -> Box<Task> {
        Box::new(move || {
            let address = NodeAddress::new(host.clone(), port);

            let future: Pin<Box<dyn Future<Output = Result<(), String>> + Send>> =
                Box::pin(async move {
                    let node_name = format!("{}:{}", address.host(), address.port());

                    info!("Starting empty node at {}", node_name);

                    let config = Arc::new(DefaultNodeConfig::new());
                    let node_id = Arc::new(DefaultNodeIdentifier::new(
                        node_name.clone(),
                        address.clone(),
                    ));

                    let state = Arc::new(DefaultNodeState::new(
                        config.clone(),
                        node_id,
                        Arc::new(DefaultRouteHandler::new()),
                    ));

                    state.init().await.map_err(|err| err.to_string())?;

                    Ok(())
                });

            future
        })
    }

    pub fn new() -> Self {
        Self { nodes: vec![] }
    }

    pub fn node(mut self) -> Self {
        self.nodes.push(InternalNode::new());
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        if self.nodes.is_empty() {
            self.nodes.push(InternalNode::new());
        }

        if let Some(node) = self.nodes.last_mut() {
            node.port = Some(port);
        }

        self
    }

    pub fn addr(mut self, addr: impl Into<String>) -> Self {
        if self.nodes.is_empty() {
            self.nodes.push(InternalNode::new());
        }

        if let Some(node) = self.nodes.last_mut() {
            node.host = addr.into();
        }

        self
    }

    pub fn build(self) -> Result<Vec<Box<Task>>, String> {
        if self.nodes.is_empty() {
            return Err("missing node in NodeBuilder".to_string());
        }

        let mut tasks = Vec::with_capacity(self.nodes.len());

        for node in self.nodes {
            let port = node.port.ok_or("missing port in NodeBuilder")?;
            let host = node.host;

            tasks.push(Self::create_empty_node_task(host, port));
        }

        Ok(tasks)
    }
}
