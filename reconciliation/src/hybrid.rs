use std::sync::Arc;

use async_trait::async_trait;
use protocol::Protocol;
use state::node::{DefaultNodeState, NodeState};

use connection::{
    node::{
        default::{
            DefaultNodeSocketTask, DefaultNodeSocketTaskMetadata, PeriodicDefaultNodeSocketTask,
        },
        port::{ConnectionInfo, NodePort},
        NodeSocketTaskMetadata, PeriodicNodeSocketTask,
    },
    route::{DefaultRouteHandler, HashMapRouteStorage, RouteHandler, RouteStorage, RouteTask},
};
use membership::{
    DefaultMembership, DefaultMembershipNeighbor, DefaultMembershipNeighborRepresentation,
    Membership, MembershipNeighbor, MembershipNeighbors,
};
use runtime::time::{PeriodTimeUnit, TokioPeriodTimeUnit};

use crate::algorithms::DefaultSimilarityLevel;
use crate::algorithms::{DefaultSimilartyLevelDetector, SimilarityLevelDetector};
use crate::ReconciliationProtocol;

pub struct HybridReconciliationProtocol {
    state: Arc<
        DefaultNodeState<
            DefaultNodeSocketTask,
            DefaultNodeSocketTaskMetadata,
            DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>,
            DefaultMembership,
            DefaultMembershipNeighbor,
            NodePort,
            u16,
            DefaultRouteHandler,
            HashMapRouteStorage,
        >,
    >,
    port: NodePort,
    similarity_level_detector:
        Arc<dyn SimilarityLevelDetector<DefaultSimilarityLevel> + Send + Sync>,
}

impl HybridReconciliationProtocol {
    pub fn new(
        state: Arc<
            DefaultNodeState<
                DefaultNodeSocketTask,
                DefaultNodeSocketTaskMetadata,
                DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>,
                DefaultMembership,
                DefaultMembershipNeighbor,
                NodePort,
                u16,
                DefaultRouteHandler,
                HashMapRouteStorage,
            >,
        >,
        port: NodePort,
    ) -> Self {
        Self {
            state,
            port,
            similarity_level_detector: Arc::new(DefaultSimilartyLevelDetector::new()),
        }
    }
}

#[async_trait]
impl<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
    Protocol<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
    for HybridReconciliationProtocol
where
    S: NodeState<T, M, N, R, MN, CI, CV, PTU, PT, RHandler, RStorage>,
    T: RouteTask,
    M: NodeSocketTaskMetadata,
    R: MembershipNeighbors<MN>,
    N: Membership<R, MN>,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV>,
    CV: Sized,
    PTU: PeriodTimeUnit + Send + Sync,
    PT: PeriodicNodeSocketTask<PTU>,
    RHandler: RouteHandler<RStorage> + Send + Sync,
    RStorage: RouteStorage,
{
    async fn init(&mut self) {
        self.state
            .add_periodic_socket_task(
                self.port.clone(),
                Arc::new(PeriodicDefaultNodeSocketTask::new(
                    Arc::new(DefaultNodeSocketTaskMetadata::new(String::new())),
                    Arc::new(move || {
                        Box::pin(async move {
                            println!("Processing connection");
                            Ok(())
                        })
                    }),
                    Arc::new(TokioPeriodTimeUnit::new(std::time::Duration::from_secs(5))),
                )),
            )
            .await
            .unwrap();
    }
}

#[async_trait]
impl<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
    ReconciliationProtocol<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
    for HybridReconciliationProtocol
where
    S: NodeState<T, M, N, R, MN, CI, CV, PTU, PT, RHandler, RStorage>,
    T: RouteTask,
    M: NodeSocketTaskMetadata,
    R: MembershipNeighbors<MN>,
    N: Membership<R, MN>,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV>,
    CV: Sized,
    PTU: PeriodTimeUnit + Send + Sync,
    PT: PeriodicNodeSocketTask<PTU>,
    RHandler: RouteHandler<RStorage> + Send + Sync,
    RStorage: RouteStorage,
{
    fn state(&self) {}
}
