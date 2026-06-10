use async_trait::async_trait;
use membership::{
    DefaultMembership, DefaultMembershipNeighbor, DefaultMembershipNeighborRepresentation,
    Membership,
};
use message::Message;
use protocol::{Protocol, deserializer::ProtocolDeserializer};
use runtime::{spawn, time::TokioPeriodTimeUnit};
use state::{
    node::{DefaultNodeState, NodeState},
    storage::StorageAction,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use connection::{
    node::{
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

use crate::{
    deserializer::ReplicationDeserializer,
    messages::{
        ReplicationMessage, ReplicationMessageType, ReplicationMessageTypeValues,
        ReplicationMessageWrapper,
    },
};

use std::marker::PhantomData;

pub const REPLICATION_PROTOCOL_ID: u64 = protocol::ProtocolId::Replication as u64;

pub struct HintedHandoffReplicationProtocol<S, T> {
    state: Arc<S>,
    port: NodeAddress,
    _marker: PhantomData<T>,
}

impl HintedHandoffReplicationProtocol<DefaultNodeState, DefaultNodeSocketTask> {
    pub fn new(state: Arc<DefaultNodeState>, port: NodeAddress) -> Self {
        Self {
            state,
            port,
            _marker: PhantomData,
        }
    }

    fn setup_storage_listener(&self) {
        let state_clone = self.state.clone();
        let port_clone = self.port.clone();

        if let Some(storage) = self.state.get_storage("default".to_string()) {
            let listener: state::storage::StorageListener = Box::new(move |item| {
                let key = item.key().to_string();
                let value = item.value().to_string();
                let state = state_clone.clone();
                let port = port_clone.clone();

                spawn!({
                    let targets = state.membership().read().await.valid_connection_targets();
                    let self_id = state.node_identifier().connection_info().clone();

                    for target in targets {
                        let msg = ReplicationMessage::new(
                            Some(REPLICATION_PROTOCOL_ID),
                            ReplicationMessageType::new(
                                ReplicationMessageTypeValues::InsertNotification,
                            ),
                            ReplicationMessageWrapper::InsertNotification(
                                key.clone(),
                                value.clone(),
                            ),
                        );
                        let _ = state
                            .send_through_socket(port.clone(), Box::new(target), Box::new(msg))
                            .await;
                    }
                });
            });

            storage.add_listener(StorageAction::Insert, listener);
        }
    }
}

pub struct ReceiveReplicationMessageTask {
    state: Arc<DefaultNodeState>,
    deserializer: ReplicationDeserializer,
}

impl ReceiveReplicationMessageTask {
    pub fn new(state: Arc<DefaultNodeState>) -> Self {
        Self {
            state,
            deserializer: ReplicationDeserializer::new(),
        }
    }
}

#[async_trait]
impl RouteTask for ReceiveReplicationMessageTask {
    fn run(self: Arc<Self>, message: Vec<u8>, _neighbor: NodeAddress) {
        let state = self.state.clone();
        let msg = self.deserializer.deserialize(message);

        spawn!({
            if let Some(replication_msg) = msg.as_any().downcast_ref::<ReplicationMessage>() {
                match replication_msg.wrapper() {
                    ReplicationMessageWrapper::InsertNotification(key, value) => {
                        info!("Received InsertNotification: {}:{}", key, value);
                        if let Some(storage) = state.get_storage("default".to_string()) {
                            let item = Box::new(state::storage::item::DefaultDataStateItem::new(
                                key.clone(),
                                value.clone(),
                            ));
                            storage.store_silent(item).await;
                        }
                    }
                }
            }
        });
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
    fn deserializer(&self) -> Arc<dyn ProtocolDeserializer> {
        Arc::new(ReplicationDeserializer::new())
    }

    fn deserialize_message(&self, bytes: Vec<u8>) -> Arc<dyn Message> {
        self.deserializer().deserialize(bytes)
    }

    fn id(&self) -> u64 {
        REPLICATION_PROTOCOL_ID
    }

    async fn init(&mut self) {
        self.setup_storage_listener();

        self.state
            .add_socket_task_and_create(
                NodeSocketRouteId::new(self.port.clone(), REPLICATION_PROTOCOL_ID),
                Arc::new(ReceiveReplicationMessageTask::new(self.state.clone())),
                Box::new(move |port: NodeAddress| {
                    Arc::new(Mutex::new(DefaultNodeSocket::new(port)))
                }),
            )
            .unwrap();

        info!("HintedHandoffReplicationProtocol initialized.");
    }
}
