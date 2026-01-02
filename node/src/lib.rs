mod config;
pub mod state;
pub mod connection;

use state::NodeState;
use protocol::Protocol;
use config::{NodeConfig, DefaultNodeConfig};

use std::sync::Arc;

pub struct Node {
    config: Box<dyn NodeConfig + Send + Sync>,
    state: Arc<dyn NodeState + Send + Sync>,
    protocols: Vec<Box<dyn Protocol + Send + Sync >>,
}

impl Node {
    pub fn new() -> Self {
        Self {
            config: Box::new(DefaultNodeConfig {}),
            protocols: vec![],
            state: Arc::new(state::DefaultNodeState::new())
        }
    }
    
    pub fn new_with_state(state: Arc<dyn NodeState + Send + Sync>) -> Self {
        Self {
            config: Box::new(DefaultNodeConfig {}),
            protocols: vec![],
            state
        }
    }
   
    pub fn add_protocol(&mut self, protocol: Box<dyn Protocol + Send + Sync>) {
        self.protocols.push(protocol);
    }

    pub async fn init(&mut self) -> Result<(), ()> {
        for protocol in self.protocols.iter_mut() {
            protocol.init();
        }

        self.state.init().await;

        Ok(())
    }
}
