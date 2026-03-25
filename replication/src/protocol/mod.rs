use async_trait::async_trait;
use membership::{
    DefaultMembership, DefaultMembershipNeighbor, DefaultMembershipNeighborRepresentation,
};
use message::Message;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use connection::{
    node::{
        NodeSocketTaskMetadata,
        default::{
            DefaultNodeSocket, DefaultNodeSocketTask, DefaultNodeSocketTaskMetadata,
            PeriodicDefaultNodeSocketTask,
        },
        port::NodeAddress,
    },
    request::handler::default::{TestMessage, TestMessageType},
    route::{
        RouteTask,
        default::{DefaultRouteHandler, HashMapRouteStorage, NodeSocketRouteId},
    },
};
use protocol::{Protocol, ProtocolIDGenerator, deserializer::ProtocolDeserializer};
use runtime::time::TokioPeriodTimeUnit;
use state::node::{DefaultNodeState, NodeState};

use std::marker::PhantomData;

pub struct HintedHandoffReplicationProtocolConfig {
    port: NodeAddress,
}

pub struct HintedHandoffReplicationProtocol<S, T> {
    id: u64,
    state: Arc<S>,
    port: NodeAddress,
    _marker: PhantomData<T>,
}

impl HintedHandoffReplicationProtocol<DefaultNodeState, DefaultNodeSocketTask> {
    pub fn new(state: Arc<DefaultNodeState>, port: NodeAddress) -> Self {
        Self {
            id: ProtocolIDGenerator::generate(),
            state,
            port,
            _marker: PhantomData,
        }
    }
}

pub struct HintedHandoffReplicationProtocolTask {}

pub struct HintedHandoffReplicationProtocolTaskMetadata {}

impl NodeSocketTaskMetadata for HintedHandoffReplicationProtocolTaskMetadata {}

#[async_trait]
impl RouteTask for HintedHandoffReplicationProtocolTask {
    fn run(&self, message: Vec<u8>, neighbor: NodeAddress) {
        info!("Running hinted handoff replication protocol task");
    }
}

pub struct HintedHandoffDeserializer {}

impl HintedHandoffDeserializer {
    pub fn new() -> Self {
        Self {}
    }
}

impl ProtocolDeserializer for HintedHandoffDeserializer {
    fn deserialize(&self, data: Vec<u8>) -> Arc<dyn Message> {
        Arc::new(TestMessage::new(Arc::new(TestMessageType::new()), None))
    }
}

#[async_trait]
impl
    Protocol<
        DefaultNodeState,
        DefaultNodeSocketTask,
        DefaultNodeSocketTaskMetadata,
        DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>,
        DefaultMembership,
        DefaultMembershipNeighbor,
        NodeAddress,
        NodeAddress,
        TokioPeriodTimeUnit,
        PeriodicDefaultNodeSocketTask,
        DefaultRouteHandler,
        HashMapRouteStorage,
    > for HintedHandoffReplicationProtocol<DefaultNodeState, DefaultNodeSocketTask>
{
    // type ProtocolDeserializer = Arc<HintedHandoffDeserializer>;
    // type ProtocolDeserializerMessage = TestMessage;

    fn deserializer(&self) -> Arc<dyn ProtocolDeserializer> {
        Arc::new(HintedHandoffDeserializer::new())
    }

    fn deserialize_message(&self, bytes: Vec<u8>) -> Arc<dyn Message> {
        self.deserializer().deserialize(bytes)
    }

    fn id(&self) -> u64 {
        self.id
    }

    async fn init(&mut self) {
        self.state
            .add_socket_task_and_create(
                NodeSocketRouteId::new(self.port.clone(), self.id()),
                Arc::new(DefaultNodeSocketTask::new(Arc::new(
                    DefaultNodeSocketTaskMetadata::new(String::new()),
                ))),
                Box::new(move |port: NodeAddress| {
                    Arc::new(Mutex::new(DefaultNodeSocket::new(port)))
                }),
            )
            .unwrap();
    }
}
