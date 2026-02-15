use config::node::NodeConfig;
use errors::node::NodeInitError;
use membership::{Membership, MembershipNeighbor, MembershipNeighbors};
use protocol::Protocol;
use runtime::time::PeriodTimeUnit;

use connection::node::{
    id::NodeIdentifier, port::ConnectionInfo, NodeSocketTask, NodeSocketTaskMetadata,
    PeriodicNodeSocketTask,
};
use state::node::NodeState;

use runtime::Runtime;

use std::marker::PhantomData;
use std::sync::Arc;

pub struct Node<T, S, M, R, N, MN, CI, CV, PTU, PT>
where
    T: NodeSocketTask<M>,
    S: NodeState<T, M, N, R, MN, CI, CV, PTU, PT>,
    M: NodeSocketTaskMetadata,
    N: Membership<R, MN>,
    R: MembershipNeighbors<MN>,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV>,
    CV: Sized,
    PTU: PeriodTimeUnit + Send + Sync,
    PT: PeriodicNodeSocketTask<PTU>,
{
    identifier: Box<dyn NodeIdentifier<CI, CV> + Send + Sync>,
    runtime: Arc<dyn Runtime + Send + Sync>,
    config: Arc<dyn NodeConfig<R, MN> + Send + Sync>,
    state: Arc<S>,
    protocols: Vec<Box<dyn Protocol<S, T, M, R, N, MN, CI, CV, PTU, PT> + Send + Sync>>,
    _marker: PhantomData<T>,
}

impl<T, S, M, R, N, MN, CI, CV, PTU, PT> Node<T, S, M, R, N, MN, CI, CV, PTU, PT>
where
    T: NodeSocketTask<M> + Send + Sync,
    S: NodeState<T, M, N, R, MN, CI, CV, PTU, PT> + Send + Sync + 'static,
    M: NodeSocketTaskMetadata,
    N: Membership<R, MN>,
    R: MembershipNeighbors<MN>,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV>,
    CV: Sized,
    PTU: PeriodTimeUnit + Send + Sync,
    PT: PeriodicNodeSocketTask<PTU>,
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
        protocol: Box<dyn Protocol<S, T, M, R, N, MN, CI, CV, PTU, PT> + Send + Sync>,
    ) {
        self.protocols.push(protocol);
    }

    pub async fn init(&mut self) -> Result<(), NodeInitError> {
        for protocol in self.protocols.iter_mut() {
            protocol.init().await;
        }

        let state_clone = self.state.clone();
        self.runtime
            .clone()
            .spawn(Box::new(move || {
                let state = state_clone.clone();
                Box::pin(async move {
                    state.init().await.unwrap();
                    Ok(())
                })
            }))
            .await;

        Ok(())
    }
}
