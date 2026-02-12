use config::node::NodeConfig;
use errors::node::NodeInitError;
use membership::{Membership, MembershipNeighbor, MembershipNeighbors};
use protocol::Protocol;

use connection::node::{
    id::NodeIdentifier, port::ConnectionInfo, NodeSocketTask, NodeSocketTaskMetadata,
};
use state::node::NodeState;

use runtime::Runtime;

use std::marker::PhantomData;
use std::sync::Arc;

pub struct Node<T, S, M, R, N, MN, CI, CV>
where
    T: NodeSocketTask<M>,
    S: NodeState<T, M, N, R, MN, CI, CV>,
    M: NodeSocketTaskMetadata,
    N: Membership<R, MN>,
    R: MembershipNeighbors<MN>,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV>,
    CV: Sized,
{
    identifier: Box<dyn NodeIdentifier<CI, CV> + Send + Sync>,
    runtime: Arc<dyn Runtime + Send + Sync>,
    config: Arc<dyn NodeConfig<R, MN> + Send + Sync>,
    state: Arc<S>,
    protocols: Vec<Box<dyn Protocol<S, T, M, R, N, MN, CI, CV> + Send + Sync>>,
    _marker: PhantomData<T>,
}

impl<T, S, M, R, N, MN, CI, CV> Node<T, S, M, R, N, MN, CI, CV>
where
    T: NodeSocketTask<M>,
    S: NodeState<T, M, N, R, MN, CI, CV>,
    M: NodeSocketTaskMetadata,
    N: Membership<R, MN>,
    R: MembershipNeighbors<MN>,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV>,
    CV: Sized,
{
    pub fn new(
        runtime: Arc<dyn Runtime + Send + Sync>,
        state: Arc<S>,
        config: Arc<dyn NodeConfig<R, MN> + Send + Sync>,
        identifier: Box<dyn NodeIdentifier<CI, CV> + Send + Sync>,
    ) -> Self {
        Self {
            identifier,
            config,
            protocols: vec![],
            state,
            runtime,
            _marker: PhantomData,
        }
    }

    pub fn add_protocol(
        &mut self,
        protocol: Box<dyn Protocol<S, T, M, R, N, MN, CI, CV> + Send + Sync>,
    ) {
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
