mod config;
pub mod state;
pub mod connection;

use protocol::Protocol;
use config::{NodeConfig, DefaultNodeConfig};

pub struct Node {
    config: Box<dyn NodeConfig>,
    protocols: Vec<Box<dyn Protocol>>,
}

impl Node {
    pub fn new() -> Self {
        Self {
            config: Box::new(DefaultNodeConfig {}),
            protocols: vec![],
        }
    }

    pub fn new_with(config: Box<dyn NodeConfig>, protocols: Vec<Box<dyn Protocol>>) -> Self {
        Self {
            config,
            protocols
        }
    }

    pub fn add_protocol(&mut self, protocol: Box<dyn Protocol>) {
        self.protocols.push(protocol);
    }

    pub fn init(&mut self) {
        for protocol in self.protocols.iter_mut() {
            protocol.init();
        }
    }
}
