use async_trait::async_trait;
use connection::{
    node::{
        default::{
            DefaultNodeSocket, DefaultNodeSocketTaskMetadata, PeriodicDefaultNodeSocketTask,
        },
        port::{ConnectionInfo, NodeAddress},
        NodeSocketTaskMetadata, PeriodicNodeSocketTask,
    },
    route::{default::NodeSocketRouteId, RouteHandler, RouteStorage, RouteTask},
};
use membership::{Membership, MembershipNeighbor, MembershipNeighbors};
use message::Message;
use protocol::{deserializer::ProtocolDeserializer, Protocol};
use runtime::time::{PeriodTimeUnit, TokioPeriodTimeUnit};
use state::{
    node::{DefaultNodeState, NodeState},
    storage::StorageAction,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use super::{
    deserializer::MerkleTreeDeserializer,
    messages::{
        MerkleTreeMessage, MerkleTreeMessageType, MerkleTreeMessageTypeValues,
        MerkleTreeMessageWrapper,
    },
    receiver::ReceiveMerkleTreeMessageTask,
    tree::BinaryMerkleTree,
};

pub const MERKLE_TREE_PROTOCOL_ID: u64 = 2;

pub struct MerkleTreeReconciliationProtocol {
    state: Arc<DefaultNodeState>,
    tree: Arc<BinaryMerkleTree>,
    port: NodeAddress,
    deserializer: Arc<MerkleTreeDeserializer>,
}

impl MerkleTreeReconciliationProtocol {
    pub fn new(state: Arc<DefaultNodeState>, port: NodeAddress) -> Self {
        Self {
            state,
            tree: Arc::new(BinaryMerkleTree::new()),
            port,
            deserializer: Arc::new(MerkleTreeDeserializer::new()),
        }
    }

    fn setup_storage_listener(&self) {
        let tree_clone = self.tree.clone();
        if let Some(storage) = self.state.get_storage("default".to_string()) {
            for item in storage.items() {
                self.tree
                    .insert(item.key().to_string(), item.value().to_string());
            }

            let listener: state::storage::StorageListener = Box::new(move |item| {
                info!("Storage updated dynamically, rebuilding Merkle Tree hashes.");
                tree_clone.insert(item.key().to_string(), item.value().to_string());
            });
            storage.add_listener(StorageAction::Insert, listener);
        }
    }

    async fn periodic_sync(
        state: Arc<DefaultNodeState>,
        protocol_id: u64,
        tree: Arc<BinaryMerkleTree>,
    ) {
        info!("Running periodic Merkle Tree sync");

        let connection_targets = state.membership().read().await.valid_connection_targets();

        if connection_targets.is_empty() {
            info!("No neighbors found for sync.");
            return;
        }

        let target = {
            use rand::RngExt;
            let mut rng = rand::rng();
            let target_idx = rng.random_range(0..connection_targets.len());
            connection_targets[target_idx].clone()
        };

        let root_hash = tree.get_root_hash();
        info!("Sending SyncRoot to {:?}", target);

        let session_id = uuid::Uuid::new_v4().to_string();
        let msg = MerkleTreeMessage::new(
            Some(protocol_id),
            MerkleTreeMessageType::new(MerkleTreeMessageTypeValues::SyncRoot),
            session_id,
            MerkleTreeMessageWrapper::SyncRoot(root_hash),
        );

        let self_id = state.node_identifier().connection_info().clone();

        let _ = state
            .send_through_socket(self_id, Box::new(target), Box::new(msg))
            .await;
    }
}

#[async_trait]
impl<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
    Protocol<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
    for MerkleTreeReconciliationProtocol
where
    S: NodeState + Send + Sync + 'static,
    T: RouteTask + Send + Sync + 'static,
    M: NodeSocketTaskMetadata + Send + Sync + 'static,
    R: MembershipNeighbors<MN> + Send + Sync + 'static,
    N: Membership<R, MN> + Send + Sync + 'static,
    MN: MembershipNeighbor + Send + Sync + 'static,
    CI: ConnectionInfo<CV> + Send + Sync + 'static,
    CV: Sized + Send + Sync + 'static,
    PTU: PeriodTimeUnit + Send + Sync + 'static,
    PT: PeriodicNodeSocketTask<PTU> + Send + Sync + 'static,
    RHandler: RouteHandler + Send + Sync + 'static,
    RStorage: RouteStorage + Send + Sync + 'static,
{
    fn deserializer(&self) -> Arc<dyn ProtocolDeserializer> {
        self.deserializer.clone()
    }

    fn deserialize_message(&self, bytes: Vec<u8>) -> Arc<dyn Message> {
        self.deserializer.deserialize(bytes)
    }

    fn id(&self) -> u64 {
        MERKLE_TREE_PROTOCOL_ID
    }

    async fn init(&mut self) {
        self.setup_storage_listener();

        let protocol_id = MERKLE_TREE_PROTOCOL_ID;
        let state_clone = self.state.clone();
        let tree_clone = self.tree.clone();

        self.state
            .add_socket_task_and_create(
                NodeSocketRouteId::new(self.port.clone(), protocol_id),
                Arc::new(ReceiveMerkleTreeMessageTask::new(
                    state_clone.node_identifier(),
                    state_clone.clone(),
                    self.tree.clone(),
                )),
                Box::new(move |port: NodeAddress| {
                    Arc::new(Mutex::new(DefaultNodeSocket::new(port)))
                }),
            )
            .unwrap();

        self.state
            .add_periodic_socket_task(
                self.port.clone(),
                Arc::new(PeriodicDefaultNodeSocketTask::new(
                    Arc::new(DefaultNodeSocketTaskMetadata::new(String::new())),
                    Arc::new(move || {
                        let state = state_clone.clone();
                        let tree = tree_clone.clone();

                        Box::pin(async move {
                            Self::periodic_sync(state, protocol_id, tree).await;
                            Ok(())
                        })
                    }),
                    Arc::new(TokioPeriodTimeUnit::new(std::time::Duration::from_secs(5))),
                )),
            )
            .await
            .unwrap();

        info!("MerkleTreeReconciliationProtocol initialized.");
    }
}
