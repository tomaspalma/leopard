use metrics::counter;
use runtime::metrics::experiment::get_context;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{error, info};

use connection::{
    node::{id::NodeIdentifier, port::NodeAddress},
    route::RouteTask,
};
use message::Message;
use protocol::deserializer::ProtocolDeserializer;
use runtime::spawn;
use state::node::{DefaultNodeState, NodeState};

use crate::merkle_tree::messages::{MerkleTreeMessageType, MerkleTreeMessageTypeValues};

use super::{
    deserializer::MerkleTreeDeserializer,
    messages::{MerkleTreeMessage, MerkleTreeMessageWrapper},
    tree::BinaryMerkleTree,
};

pub struct ReceiveMerkleTreeMessageTask {
    identifier: Arc<dyn NodeIdentifier<NodeAddress, NodeAddress> + Send + Sync>,
    state: Arc<DefaultNodeState>,
    tree: Arc<BinaryMerkleTree>,
    // Keyed by session_id (UUID). Tracks outstanding requests for each reconciliation session.
    pending: Arc<Mutex<HashMap<String, i64>>>,
}

impl ReceiveMerkleTreeMessageTask {
    pub fn new(
        identifier: Arc<dyn NodeIdentifier<NodeAddress, NodeAddress> + Send + Sync>,
        state: Arc<DefaultNodeState>,
        tree: Arc<BinaryMerkleTree>,
    ) -> Self {
        Self {
            identifier,
            state,
            tree,
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn adjust_pending(
        pending: &Arc<Mutex<HashMap<String, i64>>>,
        session_id: &str,
        delta: i64,
    ) -> bool {
        let mut guard = pending.lock().unwrap();
        if let Some(count) = guard.get_mut(session_id) {
            *count += delta;
            if *count <= 0 {
                guard.remove(session_id);
                return true;
            }
        }
        false
    }

    fn handle_message(&self, msg: &MerkleTreeMessage, neighbor: NodeAddress) {
        let session_id = msg.session_id().to_string();
        match msg.wrapper() {
            MerkleTreeMessageWrapper::SyncRoot(root_hash) => {
                self.handle_sync_root(msg.protocol(), neighbor, root_hash, session_id);
            }
            MerkleTreeMessageWrapper::SyncNodeRequest(node_id) => {
                self.handle_sync_node_request(msg.protocol(), neighbor, node_id, session_id);
            }
            MerkleTreeMessageWrapper::SyncNodeResponse(
                node_id,
                _root_hash,
                hash,
                _parent_hash,
                remote_key,
            ) => {
                self.handle_sync_node_response(
                    msg.protocol(),
                    neighbor,
                    node_id,
                    hash,
                    remote_key,
                    session_id,
                );
            }
            MerkleTreeMessageWrapper::DataRequest(key) => {
                self.handle_data_request(msg.protocol(), neighbor, key, session_id);
            }
            MerkleTreeMessageWrapper::DataResponse(key, value) => {
                self.handle_data_response(neighbor, key, value, session_id);
            }
        }
    }

    fn handle_sync_root(
        &self,
        protocol_id: Option<u64>,
        neighbor: NodeAddress,
        root_hash: &[u8; 32],
        session_id: String,
    ) {
        counter!("merkle_tree_sync_root_received", "neighbor" => format!("{:?}", neighbor))
            .increment(1);
        let local_root = self.tree.get_root_hash();
        if local_root != *root_hash {
            info!("Root hash mismatch. Requesting children nodes sync.");

            self.pending.lock().unwrap().insert(session_id.clone(), 2);

            let state_clone = self.state.clone();
            let id_clone = self.identifier.connection_info().clone();
            let neighbor_clone = neighbor.clone();

            spawn!({
                let request_left = MerkleTreeMessage::new(
                    protocol_id.clone(),
                    MerkleTreeMessageType::new(MerkleTreeMessageTypeValues::SyncNodeRequest),
                    session_id.clone(),
                    MerkleTreeMessageWrapper::SyncNodeRequest("root-left".to_string()),
                );
                let _ = state_clone
                    .send_through_socket(
                        id_clone.clone(),
                        Box::new(neighbor_clone.clone()),
                        Box::new(request_left),
                    )
                    .await;

                let request_right = MerkleTreeMessage::new(
                    protocol_id,
                    MerkleTreeMessageType::new(MerkleTreeMessageTypeValues::SyncNodeRequest),
                    session_id,
                    MerkleTreeMessageWrapper::SyncNodeRequest("root-right".to_string()),
                );
                let _ = state_clone
                    .send_through_socket(
                        id_clone,
                        Box::new(neighbor_clone),
                        Box::new(request_right),
                    )
                    .await;
            });
        } else {
            info!("Root hash match. In sync.");
            let context = get_context();
            counter!(
                "reconciliation_completed",
                "protocol" => "merkle",
                "neighbor" => format!("{:?}", neighbor),
                "run_id" => context.run_id().to_string(),
                "trial" => context.trial().to_string(),
                "similarity" => context.similarity().to_string()
            )
            .increment(1);
            runtime::metrics::csv::finish_iteration(
                format!("{:?}", self.identifier.connection_info()),
                format!("{:?}", neighbor),
                "merkle",
            );
        }
    }

    fn handle_sync_node_request(
        &self,
        protocol_id: Option<u64>,
        neighbor: NodeAddress,
        node_id: &String,
        session_id: String,
    ) {
        counter!("merkle_tree_sync_node_request_received", "neighbor" => format!("{:?}", neighbor))
            .increment(1);
        info!("Received SyncNodeRequest from {:?}", node_id);
        let root_hash = self.tree.get_root_hash();

        let (node_hash, parent_hash, node_key) =
            if let Some((node, p_hash)) = self.tree.get_node(node_id) {
                (node.hash, p_hash, node.key)
            } else {
                ([0; 32], None, None)
            };

        let state_clone = self.state.clone();
        let id_clone = self.identifier.connection_info().clone();
        let node_id_clone = node_id.clone();

        spawn!({
            let response = MerkleTreeMessage::new(
                protocol_id,
                MerkleTreeMessageType::new(MerkleTreeMessageTypeValues::SyncNodeResponse),
                session_id,
                MerkleTreeMessageWrapper::SyncNodeResponse(
                    node_id_clone,
                    root_hash,
                    node_hash,
                    parent_hash,
                    node_key,
                ),
            );
            let _ = state_clone
                .send_through_socket(id_clone, Box::new(neighbor), Box::new(response))
                .await;
        });
    }

    fn handle_sync_node_response(
        &self,
        protocol_id: Option<u64>,
        neighbor: NodeAddress,
        node_id: &String,
        hash: &[u8; 32],
        remote_key: &Option<String>,
        session_id: String,
    ) {
        counter!("merkle_tree_sync_node_response_received", "neighbor" => format!("{:?}", neighbor))
            .increment(1);
        info!("Received SyncNodeResponse for {:?}", node_id);
        let local_node = self.tree.get_node(node_id);
        let local_hash = local_node.map_or([0; 32], |(n, _)| n.hash);

        // Compute the net counter delta for this response:
        //   -1  consumed this response
        //   +N  new requests we are about to send
        let delta: i64 = if local_hash != *hash {
            if remote_key.is_some() {
                0 // -1 (response consumed) + 1 (DataRequest sent) = 0
            } else if *hash != [0; 32] {
                1 // -1 (response consumed) + 2 (SyncNodeRequests sent) = +1
            } else {
                -1 // empty remote node, no new requests
            }
        } else {
            -1 // hashes match, no new requests
        };

        let should_finish = Self::adjust_pending(&self.pending, &session_id, delta);

        if local_hash != *hash {
            let state_clone = self.state.clone();
            let id_clone = self.identifier.connection_info().clone();
            let neighbor_clone = neighbor.clone();
            let node_id_clone = node_id.clone();

            if let Some(key) = remote_key {
                info!("Leaf node mismatch. Requesting data for key: {:?}", key);
                let key_clone = key.clone();
                spawn!({
                    let request = MerkleTreeMessage::new(
                        protocol_id,
                        MerkleTreeMessageType::new(MerkleTreeMessageTypeValues::DataRequest),
                        session_id,
                        MerkleTreeMessageWrapper::DataRequest(key_clone),
                    );
                    let _ = state_clone
                        .send_through_socket(id_clone, Box::new(neighbor_clone), Box::new(request))
                        .await;
                });
            } else if *hash != [0; 32] {
                info!(
                    "Internal node mismatch at {:?}. Requesting children.",
                    node_id
                );
                spawn!({
                    let request_left = MerkleTreeMessage::new(
                        protocol_id.clone(),
                        MerkleTreeMessageType::new(MerkleTreeMessageTypeValues::SyncNodeRequest),
                        session_id.clone(),
                        MerkleTreeMessageWrapper::SyncNodeRequest(format!(
                            "{}-left",
                            node_id_clone
                        )),
                    );
                    let _ = state_clone
                        .send_through_socket(
                            id_clone.clone(),
                            Box::new(neighbor_clone.clone()),
                            Box::new(request_left),
                        )
                        .await;

                    let request_right = MerkleTreeMessage::new(
                        protocol_id,
                        MerkleTreeMessageType::new(MerkleTreeMessageTypeValues::SyncNodeRequest),
                        session_id,
                        MerkleTreeMessageWrapper::SyncNodeRequest(format!(
                            "{}-right",
                            node_id_clone
                        )),
                    );
                    let _ = state_clone
                        .send_through_socket(
                            id_clone,
                            Box::new(neighbor_clone),
                            Box::new(request_right),
                        )
                        .await;
                });
            }
        }

        if should_finish {
            let context = get_context();
            counter!(
                "reconciliation_completed",
                "protocol" => "merkle",
                "neighbor" => format!("{:?}", neighbor),
                "run_id" => context.run_id().to_string(),
                "trial" => context.trial().to_string(),
                "similarity" => context.similarity().to_string()
            )
            .increment(1);
            runtime::metrics::csv::finish_iteration(
                format!("{:?}", self.identifier.connection_info()),
                format!("{:?}", neighbor),
                "merkle",
            );
        }
    }

    fn handle_data_request(
        &self,
        protocol_id: Option<u64>,
        neighbor: NodeAddress,
        key: &String,
        session_id: String,
    ) {
        counter!("merkle_tree_data_request_received", "neighbor" => format!("{:?}", neighbor))
            .increment(1);
        info!("Received DataRequest for key {:?}", key);
        if let Some(value) = self.tree.data.read().unwrap().get(key).cloned() {
            let state_clone = self.state.clone();
            let id_clone = self.identifier.connection_info().clone();
            let key_clone = key.clone();

            spawn!({
                let response = MerkleTreeMessage::new(
                    protocol_id,
                    MerkleTreeMessageType::new(MerkleTreeMessageTypeValues::DataResponse),
                    session_id,
                    MerkleTreeMessageWrapper::DataResponse(key_clone, value),
                );
                let _ = state_clone
                    .send_through_socket(id_clone, Box::new(neighbor), Box::new(response))
                    .await;
            });
        }
    }

    fn handle_data_response(
        &self,
        neighbor: NodeAddress,
        key: &String,
        value: &String,
        session_id: String,
    ) {
        counter!("merkle_tree_data_response_received", "neighbor" => format!("{:?}", neighbor))
            .increment(1);
        info!(
            "Received DataResponse for key {:?}. Evaluating for local tree and storage insertion.",
            key
        );
        let state_clone = self.state.clone();
        let key_clone = key.clone();
        let value_clone = value.clone();
        let neighbor_clone = neighbor.clone();
        let self_addr = format!("{:?}", self.identifier.connection_info());
        let pending = self.pending.clone();

        spawn!({
            if let Some(storage) = state_clone.get_storage("default".to_string()) {
                let should_store = if let Some(existing) = storage.get(&key_clone).await {
                    value_clone > existing.value().to_string()
                } else {
                    true
                };

                if should_store {
                    info!(
                        "Updating key {:?} to new value {:?}",
                        key_clone, value_clone
                    );
                    let item = Box::new(state::storage::item::DefaultDataStateItem::new(
                        key_clone,
                        value_clone,
                    ));
                    storage.store(item).await;
                } else {
                    info!(
                        "Keeping existing value for key {:?} (conflict resolution)",
                        key_clone
                    );
                }

                let should_finish = Self::adjust_pending(&pending, &session_id, -1);

                if should_finish {
                    let context = get_context();
                    counter!(
                        "reconciliation_completed",
                        "protocol" => "merkle",
                        "neighbor" => format!("{:?}", neighbor_clone),
                        "run_id" => context.run_id().to_string(),
                        "trial" => context.trial().to_string(),
                        "similarity" => context.similarity().to_string()
                    )
                    .increment(1);
                    runtime::metrics::csv::finish_iteration(
                        self_addr,
                        format!("{:?}", neighbor_clone),
                        "merkle",
                    );
                }
            }
        });
    }
}

impl RouteTask for ReceiveMerkleTreeMessageTask {
    fn run(self: Arc<Self>, message: Vec<u8>, neighbor: NodeAddress) {
        let deserialized_message = MerkleTreeDeserializer::new().deserialize(message);

        if let Some(msg) = deserialized_message
            .as_any()
            .downcast_ref::<MerkleTreeMessage>()
        {
            let context = get_context();
            metrics::counter!(
                "protocol_round_trip_count",
                "target" => format!("{:?}", neighbor),
                "protocol" => "merkle",
                "run_id" => context.run_id().to_string(),
                "trial" => context.trial().to_string(),
                "similarity" => context.similarity().to_string()
            )
            .increment(1);

            self.handle_message(msg, neighbor);
        } else {
            error!("Failed to downcast MerkleTreeMessage");
        }
    }
}
