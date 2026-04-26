use metrics::{counter, gauge};
use runtime::metrics::experiment::get_context;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
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
    tree::{BinaryMerkleTree, MerkleTreeSnapshot},
};

pub struct ReceiveMerkleTreeMessageTask {
    identifier: Arc<dyn NodeIdentifier<NodeAddress, NodeAddress> + Send + Sync>,
    state: Arc<DefaultNodeState>,
    tree: Arc<BinaryMerkleTree>,
    // Keyed by session_id (UUID). Tracks outstanding requests for each reconciliation session.
    pending: Arc<Mutex<HashMap<String, i64>>>,
    // Keyed by session_id. Records when each reconciliation session started.
    start_times: Arc<Mutex<HashMap<String, Instant>>>,
    // Initiator-side: snapshot of the local tree taken when we first received a SyncRoot for
    // this session.  Used for all local-hash comparisons so the baseline doesn't change while
    // we are mid-traversal receiving DataResponses.
    local_snapshots: Arc<Mutex<HashMap<String, MerkleTreeSnapshot>>>,
    // Responder-side: snapshot of the local tree taken on the first SyncNodeRequest we receive
    // for a session.  All subsequent SyncNodeRequests for the same session are answered from
    // this snapshot so the requester sees a consistent tree throughout the session.
    remote_snapshots: Arc<Mutex<HashMap<String, MerkleTreeSnapshot>>>,
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
            start_times: Arc::new(Mutex::new(HashMap::new())),
            local_snapshots: Arc::new(Mutex::new(HashMap::new())),
            remote_snapshots: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // Returns the round duration if the session just completed, None if still in progress.
    fn adjust_pending(
        pending: &Arc<Mutex<HashMap<String, i64>>>,
        start_times: &Arc<Mutex<HashMap<String, Instant>>>,
        session_id: &str,
        delta: i64,
    ) -> Option<f64> {
        let mut guard = pending.lock().unwrap();
        if let Some(count) = guard.get_mut(session_id) {
            *count += delta;
            if *count <= 0 {
                guard.remove(session_id);
                return Some(
                    start_times
                        .lock()
                        .unwrap()
                        .remove(session_id)
                        .map_or(0.0, |t| t.elapsed().as_secs_f64()),
                );
            }
        }
        None
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
        let start = Instant::now();
        let local_root = self.tree.get_root_hash();
        if local_root != *root_hash {
            info!("Root hash mismatch. Requesting children nodes sync.");

            // Freeze a snapshot of our local tree for this session.  All hash comparisons in
            // handle_sync_node_response will use this snapshot so that DataResponses arriving
            // mid-session and rebuilding our live tree don't change the comparison baseline.
            self.local_snapshots
                .lock()
                .unwrap()
                .insert(session_id.clone(), self.tree.snapshot());

            self.pending.lock().unwrap().insert(session_id.clone(), 2);
            self.start_times.lock().unwrap().insert(session_id.clone(), start);

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
            gauge!(
                "reconciliation_round_duration_seconds",
                "protocol" => "merkle",
                "neighbor" => format!("{:?}", neighbor),
                "run_id" => context.run_id().to_string(),
                "trial" => context.trial().to_string(),
                "similarity" => context.similarity().to_string()
            )
            .set(start.elapsed().as_secs_f64());
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

        // On the first request for this session, freeze a snapshot of our tree.
        // Every subsequent request for the same session is answered from that
        // snapshot so the requester always sees a consistent tree.
        {
            let mut snaps = self.remote_snapshots.lock().unwrap();
            if !snaps.contains_key(&session_id) {
                snaps.insert(session_id.clone(), self.tree.snapshot());
            }
        }

        let (root_hash, node_hash, parent_hash, node_key) = {
            let snaps = self.remote_snapshots.lock().unwrap();
            let snap = &snaps[&session_id];
            let root_hash = snap.get_root_hash();
            let (node_hash, parent_hash, node_key) =
                if let Some((node, p_hash)) = snap.get_node(node_id) {
                    (node.hash, p_hash, node.key)
                } else {
                    ([0; 32], None, None)
                };
            (root_hash, node_hash, parent_hash, node_key)
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
        // Use the session snapshot for comparison so that Ka/Kb keys received via
        // DataResponse mid-session (which rebuild the live tree) don't change the
        // baseline and create false hash matches.
        let local_hash = {
            let snaps = self.local_snapshots.lock().unwrap();
            if let Some(snap) = snaps.get(&session_id) {
                snap.get_node(node_id).map_or([0; 32], |(n, _)| n.hash)
            } else {
                self.tree.get_node(node_id).map_or([0; 32], |(n, _)| n.hash)
            }
        };

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

        let round_duration = Self::adjust_pending(&self.pending, &self.start_times, &session_id, delta);
        let local_snapshots = self.local_snapshots.clone();
        let session_id_for_cleanup = session_id.clone();

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

        if let Some(duration) = round_duration {
            local_snapshots.lock().unwrap().remove(&session_id_for_cleanup);
            let context = get_context();
            gauge!(
                "reconciliation_round_duration_seconds",
                "protocol" => "merkle",
                "neighbor" => format!("{:?}", neighbor),
                "run_id" => context.run_id().to_string(),
                "trial" => context.trial().to_string(),
                "similarity" => context.similarity().to_string()
            )
            .set(duration);
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
        let start_times = self.start_times.clone();
        let local_snapshots = self.local_snapshots.clone();

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

                if let Some(duration) = Self::adjust_pending(&pending, &start_times, &session_id, -1) {
                    local_snapshots.lock().unwrap().remove(&session_id);
                    let context = get_context();
                    gauge!(
                        "reconciliation_round_duration_seconds",
                        "protocol" => "merkle",
                        "neighbor" => format!("{:?}", neighbor_clone),
                        "run_id" => context.run_id().to_string(),
                        "trial" => context.trial().to_string(),
                        "similarity" => context.similarity().to_string()
                    )
                    .set(duration);
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
