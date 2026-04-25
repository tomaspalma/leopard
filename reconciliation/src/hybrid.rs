use std::sync::Arc;
use tracing::info;

use async_trait::async_trait;
use message::{Message};
use protocol::{deserializer::ProtocolDeserializer, Protocol, ProtocolIDGenerator};
use state::node::{DefaultNodeState, NodeState};

use connection::{
    node::{
        default::{DefaultNodeSocketTaskMetadata, PeriodicDefaultNodeSocketTask},
        port::{ConnectionInfo, NodeAddress},
        NodeSocketTaskMetadata, PeriodicNodeSocketTask,
    },
    request::handler::default::{TestMessage, TestMessageType},
    route::{RouteHandler, RouteStorage, RouteTask},
};
use membership::{Membership, MembershipNeighbor, MembershipNeighbors};
use runtime::time::{PeriodTimeUnit, TokioPeriodTimeUnit};

use crate::algorithms::DefaultSimilarityLevel;
use crate::algorithms::{DefaultSimilartyLevelDetector, SimilarityLevelDetector};
use crate::ReconciliationProtocol;

#[derive(Default)]
pub struct HybridReconciliationProtocolDeserializer {}

impl ProtocolDeserializer for HybridReconciliationProtocolDeserializer {
    fn deserialize(&self, _bytes: Vec<u8>) -> Arc<dyn Message + Send + Sync> {
        Arc::new(TestMessage::new(Arc::new(TestMessageType::new()), None))
    }
}

pub struct HybridReconciliationProtocol {
    id: u64,
    deserializer: Arc<HybridReconciliationProtocolDeserializer>,
    state: Arc<DefaultNodeState>,
    port: NodeAddress,
    similarity_level_detector:
        Arc<dyn SimilarityLevelDetector<DefaultSimilarityLevel> + Send + Sync>,
}

impl HybridReconciliationProtocol {
    pub fn new(state: Arc<DefaultNodeState>, port: NodeAddress) -> Self {
        Self {
            id: ProtocolIDGenerator::generate(),
            state,
            port,
            deserializer: Arc::new(HybridReconciliationProtocolDeserializer::default()),
            similarity_level_detector: Arc::new(DefaultSimilartyLevelDetector::new()),
        }
    }
}

#[async_trait]
impl<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
    Protocol<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
    for HybridReconciliationProtocol
where
    S: NodeState,
    T: RouteTask,
    M: NodeSocketTaskMetadata,
    R: MembershipNeighbors<MN>,
    N: Membership<R, MN>,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV>,
    CV: Sized,
    PTU: PeriodTimeUnit + Send + Sync,
    PT: PeriodicNodeSocketTask<PTU>,
    RHandler: RouteHandler + Send + Sync,
    RStorage: RouteStorage,
{
    fn deserializer(&self) -> Arc<dyn ProtocolDeserializer> {
        self.deserializer.clone()
    }

    fn deserialize_message(&self, bytes: Vec<u8>) -> Arc<dyn Message> {
        self.deserializer.deserialize(bytes)
    }

    async fn init(&mut self) {
        self.state
            .add_periodic_socket_task(
                self.port.clone(),
                Arc::new(PeriodicDefaultNodeSocketTask::new(
                    Arc::new(DefaultNodeSocketTaskMetadata::new(String::new())),
                    Arc::new(move || {
                        Box::pin(async move {
                            info!("Processing connection");
                            Ok(())
                        })
                    }),
                    Arc::new(TokioPeriodTimeUnit::new(std::time::Duration::from_secs(5))),
                )),
            )
            .await
            .unwrap();
    }

    fn id(&self) -> u64 {
        self.id
    }
}

#[async_trait]
impl<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
    ReconciliationProtocol<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
    for HybridReconciliationProtocol
where
    S: NodeState,
    T: RouteTask,
    M: NodeSocketTaskMetadata,
    R: MembershipNeighbors<MN>,
    N: Membership<R, MN>,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV>,
    CV: Sized,
    PTU: PeriodTimeUnit + Send + Sync,
    PT: PeriodicNodeSocketTask<PTU>,
    RHandler: RouteHandler + Send + Sync,
    RStorage: RouteStorage,
{
    fn state(&self) {}
}
