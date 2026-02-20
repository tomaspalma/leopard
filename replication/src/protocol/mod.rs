use async_trait::async_trait;
use membership::{
    DefaultMembership, DefaultMembershipNeighbor, DefaultMembershipNeighborRepresentation,
};
use message::DefaultMessageType;
use std::sync::Arc;

use connection::node::{
    NodeSocketTask, NodeSocketTaskMetadata,
    default::{
        DefaultNodeSocket, DefaultNodeSocketTask, DefaultNodeSocketTaskMetadata,
        PeriodicDefaultNodeSocketTask,
    },
    port::NodePort,
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
            DefaultMessageType,
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
                DefaultMessageType,
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
impl NodeSocketTask<HintedHandoffReplicationProtocolTaskMetadata>
    for HintedHandoffReplicationProtocolTask
{
    async fn run(&self) {
        println!("Running HintedHandoffReplicationProtocolTask");
    }

    fn metadata(&self) -> Arc<HintedHandoffReplicationProtocolTaskMetadata> {
        Arc::new(HintedHandoffReplicationProtocolTaskMetadata {})
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
            DefaultMessageType,
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
        DefaultMessageType,
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
            DefaultMessageType,
        >,
        DefaultNodeSocketTask,
    >
{
    async fn init(&mut self) {
        let runtime = self.state.runtime();
        self.state
            .add_socket_task_and_create(
                self.port.clone(),
                Box::new(DefaultNodeSocketTask::new(Arc::new(
                    DefaultNodeSocketTaskMetadata::new(String::new()),
                ))),
                Box::new(move |port: NodePort| {
                    Box::new(DefaultNodeSocket::new(port, runtime.clone()))
                }),
            )
            .unwrap();

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
