use async_trait::async_trait;
use membership::{
    DefaultMembership, DefaultMembershipNeighbor, DefaultMembershipNeighborRepresentation,
};
use message::Message;
use std::sync::Arc;

use connection::{
    node::{
        NodeSocketTaskMetadata,
        default::{
            DefaultNodeSocket, DefaultNodeSocketTask, DefaultNodeSocketTaskMetadata,
            PeriodicDefaultNodeSocketTask,
        },
        port::NodePort,
    },
    route::{DefaultRouteHandler, HashMapRouteStorage, NodeSocketRouteId, RouteTask},
};
use protocol::Protocol;
use runtime::time::TokioPeriodTimeUnit;
use state::node::{DefaultNodeState, NodeState};

use std::marker::PhantomData;

pub struct HintedHandoffReplicationProtocolConfig {
    port: NodePort,
}

pub struct HintedHandoffReplicationProtocol<S, T> {
    state: Arc<S>,
    port: NodePort,
    _marker: PhantomData<T>,
}

impl
    HintedHandoffReplicationProtocol<
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
        DefaultNodeSocketTask,
    >
{
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
            _marker: PhantomData,
        }
    }
}

pub struct HintedHandoffReplicationProtocolTask {}

pub struct HintedHandoffReplicationProtocolTaskMetadata {}

impl NodeSocketTaskMetadata for HintedHandoffReplicationProtocolTaskMetadata {}

#[async_trait]
impl RouteTask for HintedHandoffReplicationProtocolTask {
    fn run(&self, message: Arc<dyn Message + Send + Sync>) {
        println!("Running hinted handoff replication protocol task");
    }
}

#[async_trait]
impl
    Protocol<
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
        DefaultNodeSocketTask,
        DefaultNodeSocketTaskMetadata,
        DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>,
        DefaultMembership,
        DefaultMembershipNeighbor,
        NodePort,
        u16,
        TokioPeriodTimeUnit,
        PeriodicDefaultNodeSocketTask,
        DefaultRouteHandler,
        HashMapRouteStorage,
    >
    for HintedHandoffReplicationProtocol<
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
        DefaultNodeSocketTask,
    >
{
    async fn init(&mut self) {
        self.state
            .add_socket_task_and_create(
                NodeSocketRouteId::new(self.port.clone(), self.id()),
                Box::new(DefaultNodeSocketTask::new(Arc::new(
                    DefaultNodeSocketTaskMetadata::new(String::new()),
                ))),
                Box::new(move |port: NodePort| Box::new(DefaultNodeSocket::new(port))),
            )
            .unwrap();
    }
}
