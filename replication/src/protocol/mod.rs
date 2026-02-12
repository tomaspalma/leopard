use log::trace;
use membership::{
    DefaultMembership, DefaultMembershipNeighbor, DefaultMembershipNeighborRepresentation,
};
use std::sync::Arc;

use connection::node::{
    NodeSocketTask, NodeSocketTaskMetadata,
    default::{DefaultNodeSocket, DefaultNodeSocketTask, DefaultNodeSocketTaskMetadata},
    port::NodePort,
};
use protocol::Protocol;
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

impl NodeSocketTask<HintedHandoffReplicationProtocolTaskMetadata>
    for HintedHandoffReplicationProtocolTask
{
    fn run(&self) {
        trace!("Running HintedHandoffReplicationProtocolTask");
    }

    fn metadata(&self) -> Arc<HintedHandoffReplicationProtocolTaskMetadata> {
        Arc::new(HintedHandoffReplicationProtocolTaskMetadata {})
    }
}

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
        >,
        DefaultNodeSocketTask,
        DefaultNodeSocketTaskMetadata,
        DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>,
        DefaultMembership,
        DefaultMembershipNeighbor,
        NodePort,
        u16,
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
        >,
        DefaultNodeSocketTask,
    >
{
    fn init(&mut self) {
        self.state.add_socket_task_and_create(
            self.port.clone(),
            Box::new(DefaultNodeSocketTask::new(Arc::new(
                DefaultNodeSocketTaskMetadata::new(String::new()),
            ))),
            Box::new(|port: NodePort| {
                Box::new(DefaultNodeSocket::<DefaultNodeSocketTask>::new(port))
            }),
        );
    }
}
