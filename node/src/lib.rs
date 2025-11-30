use protocol::Protocol;
use config::NodeConfig;

struct Node {
    config: NodeConfig,
    protocols: Vec<Protocol>,
}

impl Node {
    fn new(config: NodeConfig, protocols: Vec<Protocol>) -> Self {
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