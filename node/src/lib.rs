use config::node::NodeConfig;
use connection::route::{RouteHandler, RouteStorage, RouteTask};
use errors::node::NodeInitError;
use membership::{Membership, MembershipNeighbor, MembershipNeighbors};
use protocol::Protocol;
use runtime::time::PeriodTimeUnit;

use connection::node::{
    id::NodeIdentifier, port::ConnectionInfo, NodeSocketTaskMetadata, PeriodicNodeSocketTask,
};
use state::node::NodeState;

use services::NodeService;

use std::marker::PhantomData;
use std::sync::Arc;

use runtime::spawn;

pub struct Node<T, S, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
where
    T: RouteTask,
    S: NodeState,
    M: NodeSocketTaskMetadata,
    N: Membership<R, MN>,
    R: MembershipNeighbors<MN>,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV>,
    CV: Sized,
    PTU: PeriodTimeUnit + Send + Sync,
    PT: PeriodicNodeSocketTask<PTU>,
    RHandler: RouteHandler + Send + Sync,
    RStorage: RouteStorage,
{
    identifier: Box<dyn NodeIdentifier<CI, CV> + Send + Sync>,
    config: Arc<dyn NodeConfig<R, MN> + Send + Sync>,
    state: Arc<S>,
    protocols: Vec<
        Box<dyn Protocol<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage> + Send + Sync>,
    >,
    services: Vec<Arc<dyn NodeService + Send + Sync>>,
    _marker: PhantomData<T>,
}

impl<T, S, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
    Node<T, S, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
where
    T: RouteTask + Send + Sync,
    S: NodeState + Send + Sync + 'static,
    M: NodeSocketTaskMetadata,
    N: Membership<R, MN>,
    R: MembershipNeighbors<MN>,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV>,
    CV: Sized,
    PTU: PeriodTimeUnit + Send + Sync,
    PT: PeriodicNodeSocketTask<PTU>,
    RHandler: RouteHandler + Send + Sync,
    RStorage: RouteStorage,
{
    pub fn new(
        state: Arc<S>,
        config: Arc<dyn NodeConfig<R, MN> + Send + Sync>,
        identifier: Box<dyn NodeIdentifier<CI, CV> + Send + Sync>,
    ) -> Self {
        Self {
            identifier,
            config,
            protocols: vec![],
            services: vec![],
            state,
            _marker: PhantomData,
        }
    }

    pub fn add_protocol(
        &mut self,
        protocol: Box<
            dyn Protocol<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage> + Send + Sync,
        >,
    ) {
        self.protocols.push(protocol);
    }

    pub fn add_service(&mut self, service: Arc<dyn NodeService + Send + Sync>) {
        self.services.push(service);
    }

    pub async fn init(&mut self) -> Result<(), NodeInitError> {
        for protocol in self.protocols.iter_mut() {
            protocol.init().await;
        }

        let state_clone = self.state.clone();

        spawn!({
            state_clone.init().await.unwrap();
        });

        for service in self.services.iter_mut() {
            let s = service.clone();
            spawn!({
                s.clone().init().await;
            });
        }

        Ok(())
    }
}
