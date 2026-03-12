use async_trait::async_trait;
use membership::{
    DefaultMembership, DefaultMembershipNeighbor, DefaultMembershipNeighborRepresentation,
};
use message::Message;
use std::sync::Arc;
use tokio::sync::Mutex;

use connection::{
    node::{
        NodeSocketTaskMetadata,
        default::{
            DefaultNodeSocket, DefaultNodeSocketTask, DefaultNodeSocketTaskMetadata,
            PeriodicDefaultNodeSocketTask,
        },
        port::NodeAddress,
    },
    route::{
        RouteTask,
        default::{DefaultRouteHandler, HashMapRouteStorage, NodeSocketRouteId},
    },
};
use protocol::{Protocol, ProtocolIDGenerator};
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
    fn run(&self, message: Vec<u8>) {
        println!("Running hinted handoff replication protocol task");
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
    fn id(&self) -> u64 {
        self.id
    }

    async fn init(&mut self) {
        self.state
            .add_socket_task_and_create(
                NodeSocketRouteId::new(self.port.clone(), self.id()),
                Box::new(DefaultNodeSocketTask::new(Arc::new(
                    DefaultNodeSocketTaskMetadata::new(String::new()),
                ))),
                Box::new(move |port: NodeAddress| {
                    Arc::new(Mutex::new(DefaultNodeSocket::new(port)))
                }),
            )
            .unwrap();
    }
}
