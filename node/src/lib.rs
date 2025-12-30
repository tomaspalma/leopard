mod config;
pub mod state;
pub mod connection;

use state::NodeState;
use protocol::Protocol;
use config::{NodeConfig, DefaultNodeConfig};

pub struct Node {
    config: Box<dyn NodeConfig + Send + Sync>,
    state: Box<dyn NodeState + Send + Sync>,
    protocols: Vec<Box<dyn Protocol + Send + Sync >>,
}

impl Node {
    pub fn new() -> Self {
        Self {
            config: Box::new(DefaultNodeConfig {}),
            protocols: vec![],
            state: Box::new(state::DefaultNodeState::new())
        }
    }

    pub fn new_with(config: Box<dyn NodeConfig + Send + Sync>, protocols: Vec<Box<dyn Protocol + Send + Sync>>, state: Box<dyn NodeState + Send + Sync>) -> Self {
        Self {
            config,
            protocols,
            state
        }
    }

    pub fn add_protocol(&mut self, protocol: Box<dyn Protocol + Send + Sync>) {
        self.protocols.push(protocol);
    }

    pub async fn init(&mut self) -> Result<(), ()> {
        self.state.init().await;

        for protocol in self.protocols.iter_mut() {
            protocol.init();
        }

        Ok(())
    }
}
