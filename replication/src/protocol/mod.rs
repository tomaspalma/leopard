use log::{trace};
use std::sync::Arc;

use protocol::Protocol;
use connection::node::{NodeSocketTask, port::NodePort};
use state::node::NodeState;

use std::marker::PhantomData;

pub struct HintedHandoffReplicationProtocolConfig {
    port: NodePort
}

pub trait ReplicationProtocol<S, T> : Protocol<S, T> 
where
    T: NodeSocketTask,
    S: NodeState<T> 
{

}

pub struct HintedHandoffReplicationProtocol<S, T> {
    state: Arc<S>,
    port: NodePort,
    _marker: PhantomData<T>
}

impl<S, T> ReplicationProtocol<S, T> for HintedHandoffReplicationProtocol<S, T> 
where
    T: NodeSocketTask,
    S: NodeState<T>
{}

impl<S, T> HintedHandoffReplicationProtocol<S, T> 
where
    T: NodeSocketTask,
    S: NodeState<T>
{
    pub fn new(state: Arc<S>, port: NodePort) -> Self {
        Self {
            state,
            port,
            _marker: PhantomData
        }
    }
}

pub struct HintedHandoffReplicationProtocolTask {}

pub struct HintedHandoffReplicationProtocolTaskMetadata {}

impl NodeSocketTask for HintedHandoffReplicationProtocolTask {
    fn run(&self) {
        trace!("Running HintedHandoffReplicationProtocolTask");
    }
}

impl<S: NodeState<T>, T: NodeSocketTask> Protocol<S, T> for HintedHandoffReplicationProtocol<S, T> {
    fn init(&mut self) {
        trace!("Initializing HintedHandoffReplicationProtocol");
        // self.state.add_socket_task_and_create(self.port.clone(), Box::new(HintedHandoffReplicationProtocolTask {}));
    }
}
