use log::{trace};

use protocol::Protocol;
use node::{connection::{NodeSocketTask, port::NodePort}, state::NodeState};

pub struct HintedHandoffReplicationProtocolConfig {
    port: NodePort
}

pub trait ReplicationProtocol : Protocol {}

pub struct HintedHandoffReplicationProtocol {
    state: Box<dyn NodeState>,
    port: NodePort
}

impl ReplicationProtocol for HintedHandoffReplicationProtocol {}

impl HintedHandoffReplicationProtocol {
    pub fn new(state: Box<dyn NodeState>, port: NodePort) -> Self {
        Self {
            state,
            port
        }
    }
}

impl Protocol for HintedHandoffReplicationProtocol {
    fn init(&mut self) {
        trace!("Initializing HintedHandoffReplicationProtocol");
        self.state.add_socket_task_and_create(self.port.clone(), Box::new(HintedHandoffReplicationProtocolTask {}));
    }
}

pub struct HintedHandoffReplicationProtocolTask {}

impl NodeSocketTask for HintedHandoffReplicationProtocolTask {
    fn run(&self) {
        trace!("Running HintedHandoffReplicationProtocolTask");

        println!("Running HintedHandoffReplicationProtocolTask");
    }
}
