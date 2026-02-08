mod config;

use config::{DefaultNodeConfig, NodeConfig};
use errors::node::NodeInitError;
use membership::{Membership, MembershipNeighbor, MembershipNeighbors};
use protocol::Protocol;

use connection::node::{NodeSocketTask, NodeSocketTaskMetadata};
use state::node::NodeState;

use runtime::Runtime;

use std::marker::PhantomData;
use std::sync::Arc;

pub struct Node<T, S, M, R, N, MN>
where
    T: NodeSocketTask<M>,
    S: NodeState<T, M, N, R, MN>,
    M: NodeSocketTaskMetadata,
    N: Membership<R, MN>,
    R: MembershipNeighbors,
    MN: MembershipNeighbor,
{
    runtime: Arc<dyn Runtime + Send + Sync>,
    config: Box<dyn NodeConfig + Send + Sync>,
    state: Arc<S>,
    protocols: Vec<Box<dyn Protocol<S, T, M, R, N, MN> + Send + Sync>>,
    _marker: PhantomData<T>,
}

impl<T, S, M, R, N, MN> Node<T, S, M, R, N, MN>
where
    T: NodeSocketTask<M>,
    S: NodeState<T, M, N, R, MN>,
    M: NodeSocketTaskMetadata,
    N: Membership<R, MN>,
    R: MembershipNeighbors,
    MN: MembershipNeighbor,
{
    pub fn new(runtime: Arc<dyn Runtime + Send + Sync>, state: Box<dyn Fn() -> S>) -> Self {
        Self {
            config: Box::new(DefaultNodeConfig {}),
            protocols: vec![],
            state: Arc::new(state()),
            runtime,
            _marker: PhantomData,
        }
    }

    pub fn new_with_state(state: Arc<S>, runtime: Arc<dyn Runtime + Sync + Send>) -> Self {
        Self {
            config: Box::new(DefaultNodeConfig {}),
            protocols: vec![],
            state,
            runtime,
            _marker: PhantomData,
        }
    }

    pub fn add_protocol(&mut self, protocol: Box<dyn Protocol<S, T, M, R, N, MN> + Send + Sync>) {
        self.protocols.push(protocol);
    }

    pub async fn init(&mut self) -> Result<(), NodeInitError> {
        for protocol in self.protocols.iter_mut() {
            protocol.init();
        }

        self.state.init().await?;

        Ok(())
    }
}
