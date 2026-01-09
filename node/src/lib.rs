mod config;

use protocol::Protocol;
use config::{NodeConfig, DefaultNodeConfig};

use state::node::NodeState;
use connection::node::{NodeSocketTask, NodeSocketTaskMetadata};

use runtime::{Runtime};

use std::sync::Arc;
use std::marker::PhantomData;

pub struct Node<T, S, M> 
where
    T: NodeSocketTask<M>,
    S: NodeState<T, M>,
    M: NodeSocketTaskMetadata
{
    runtime: Arc<dyn Runtime+ Send + Sync>,
    config: Box<dyn NodeConfig + Send + Sync>,
    state: Arc<S>,
    protocols: Vec<Box<dyn Protocol<S, T, M> + Send + Sync >>,
    _marker: PhantomData<T>
}

impl<T, S, M> Node<T, S, M> 
where
    T: NodeSocketTask<M>,
    S: NodeState<T, M>,
    M: NodeSocketTaskMetadata
{
    pub fn new(runtime: Arc<dyn Runtime + Send + Sync>, state: Box<dyn Fn () -> S>) -> Self {
        Self {
            config: Box::new(DefaultNodeConfig {}),
            protocols: vec![],
            state: Arc::new(state()),
            runtime,
            _marker: PhantomData
        }
    }
    
    pub fn new_with_state(state: Arc<S>, runtime: Arc<dyn Runtime + Sync + Send>) -> Self {
        Self {
            config: Box::new(DefaultNodeConfig {}),
            protocols: vec![],
            state,
            runtime,
            _marker: PhantomData
        }
    }
   
    pub fn add_protocol(&mut self, protocol: Box<dyn Protocol<S, T, M> + Send + Sync>) {
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
