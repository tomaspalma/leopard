use protocol::Protocol;
use node::{state::NodeState, connection::NodeSocketTask};

pub trait ReplicationProtocol : Protocol {}

pub struct HintedHandoffReplicationProtocol {
    state: Box<dyn NodeState>
}

impl Protocol for HintedHandoffReplicationProtocol {
    fn init(&self) {
        println!("Initializing HintedHandoffReplicationProtocol");
    }
}

impl ReplicationProtocol for HintedHandoffReplicationProtocol {}

impl HintedHandoffReplicationProtocol {
    pub fn new(state: Box<dyn NodeState>) -> Self {
        Self {
            state
        }
    }
}

pub struct HintedHandoffReplicationProtocolTask {}

impl NodeSocketTask for HintedHandoffReplicationProtocolTask {
    fn run(&self) {
        println!("Running HintedHandoffReplicationProtocolTask");
    }
}
