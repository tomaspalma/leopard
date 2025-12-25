mod config;

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

    fn init(&self) {
        for protocol in self.protocols.iter() {
            protocol.init();
        }
    }
}
