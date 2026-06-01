use metrics::{counter, gauge};
use runtime::metrics::experiment::get_context;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::oneshot;
use tokio::time::{timeout, Duration};
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
    messages::{MerkleNodeAnswer, MerkleTreeMessage, MerkleTreeMessageWrapper},
    tree::{BinaryMerkleTree, MerkleTreeSnapshot},
};

// Maximum node-ids carried in a single SyncNodeRequest. A whole tree level is
// split into chunks of this size so no single message becomes huge at low
// similarity, where the frontier can approach n/2 nodes.
const FRONTIER_CHUNK: usize = 2048;
// Maximum keys carried in a single DataRequest.
const DATA_CHUNK: usize = 2048;
// Safety net: how long the initiator waits for a batched response before giving
// up on that batch (the periodic sync will retry the session later).
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

pub struct ReceiveMerkleTreeMessageTask {
    identifier: Arc<dyn NodeIdentifier<NodeAddress, NodeAddress> + Send + Sync>,
    state: Arc<DefaultNodeState>,
    tree: Arc<BinaryMerkleTree>,
    // Responder-side: snapshot of the local tree taken on the first request we
    // receive for a session, so the initiator sees a consistent tree throughout.
    remote_snapshots: Arc<Mutex<HashMap<String, MerkleTreeSnapshot>>>,
    // Initiator-side: oneshot senders that deliver a batched response back to the
    // driver task awaiting it, keyed by request id.
    node_waiters: Arc<Mutex<HashMap<u64, oneshot::Sender<Vec<MerkleNodeAnswer>>>>>,
    data_waiters: Arc<Mutex<HashMap<u64, oneshot::Sender<Vec<(String, String)>>>>>,
    next_request_id: Arc<AtomicU64>,
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
            remote_snapshots: Arc::new(Mutex::new(HashMap::new())),
            node_waiters: Arc::new(Mutex::new(HashMap::new())),
            data_waiters: Arc::new(Mutex::new(HashMap::new())),
            next_request_id: Arc::new(AtomicU64::new(1)),
        }
    }

    fn handle_message(self: &Arc<Self>, msg: &MerkleTreeMessage, neighbor: NodeAddress) {
        let session_id = msg.session_id().to_string();
        match msg.wrapper() {
            MerkleTreeMessageWrapper::SyncRoot(root_hash) => {
                self.handle_sync_root(msg.protocol(), neighbor, root_hash, session_id);
            }
            MerkleTreeMessageWrapper::SyncNodeRequest(request_id, node_ids) => {
                self.handle_sync_node_request(
                    msg.protocol(),
                    neighbor,
                    *request_id,
                    node_ids.clone(),
                    session_id,
                );
            }
            MerkleTreeMessageWrapper::SyncNodeResponse(request_id, answers) => {
                if let Some(tx) = self.node_waiters.lock().unwrap().remove(request_id) {
                    let _ = tx.send(answers.clone());
                }
            }
            MerkleTreeMessageWrapper::DataRequest(request_id, keys) => {
                self.handle_data_request(
                    msg.protocol(),
                    neighbor,
                    *request_id,
                    keys.clone(),
                    session_id,
                );
            }
            MerkleTreeMessageWrapper::DataResponse(request_id, pairs) => {
                if let Some(tx) = self.data_waiters.lock().unwrap().remove(request_id) {
                    let _ = tx.send(pairs.clone());
                }
            }
        }
    }

    /// Initiator side: on a root mismatch, drive the whole diff in one task that
    /// descends the tree level by level. On a match, record completion.
    fn handle_sync_root(
        self: &Arc<Self>,
        protocol_id: Option<u64>,
        neighbor: NodeAddress,
        root_hash: &[u8; 32],
        session_id: String,
    ) {
        counter!("merkle_tree_sync_root_received", "neighbor" => format!("{:?}", neighbor))
            .increment(1);

        if self.tree.get_root_hash() == *root_hash {
            info!("Root hash match. In sync.");
            self.record_completion(&neighbor, 0.0);
            return;
        }

        info!("Root hash mismatch. Starting level-by-level sync.");
        let snapshot = self.tree.snapshot();
        let this = self.clone();
        spawn!({
            this.run_diff_session(protocol_id, neighbor, session_id, snapshot)
                .await;
        });
    }

    /// Walk the tree one level at a time, batching each level's node-ids into
    /// capped messages, until the differing ranges narrow to leaves; then
    /// batch-fetch those keys and apply them.
    async fn run_diff_session(
        self: Arc<Self>,
        protocol_id: Option<u64>,
        neighbor: NodeAddress,
        session_id: String,
        snapshot: MerkleTreeSnapshot,
    ) {
        let start = Instant::now();
        // Root is already known to mismatch; start there so a single-leaf remote
        // tree (root is itself a leaf) is handled by the leaf branch below.
        let mut frontier: Vec<String> = vec!["root".to_string()];
        let mut keys_to_fetch: Vec<String> = Vec::new();

        while !frontier.is_empty() {
            let mut next: Vec<String> = Vec::new();
            for chunk in frontier.chunks(FRONTIER_CHUNK) {
                let answers = self
                    .request_nodes(protocol_id, &neighbor, &session_id, chunk.to_vec())
                    .await;
                for (node_id, remote_hash, remote_key) in answers {
                    let local_hash =
                        snapshot.get_node_summary(&node_id).map_or([0; 32], |(h, _)| h);
                    if local_hash == remote_hash {
                        // Identical range: prune the whole subtree.
                        continue;
                    }
                    if let Some(key) = remote_key {
                        // Differing leaf: fetch its value.
                        keys_to_fetch.push(key);
                    } else if remote_hash != [0; 32] {
                        // Differing internal node: descend into both halves.
                        next.push(format!("{}-left", node_id));
                        next.push(format!("{}-right", node_id));
                    }
                    // remote_hash == [0;32]: absent on the remote; nothing to pull.
                }
            }
            frontier = next;
        }

        let mut fetched: Vec<(String, String)> = Vec::new();
        for chunk in keys_to_fetch.chunks(DATA_CHUNK) {
            let pairs = self
                .request_data(protocol_id, &neighbor, &session_id, chunk.to_vec())
                .await;
            fetched.extend(pairs);
        }

        self.apply_fetched(fetched).await;
        self.record_completion(&neighbor, start.elapsed().as_secs_f64());
    }

    /// Send one batched SyncNodeRequest and await the matching response.
    async fn request_nodes(
        &self,
        protocol_id: Option<u64>,
        neighbor: &NodeAddress,
        session_id: &str,
        node_ids: Vec<String>,
    ) -> Vec<MerkleNodeAnswer> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.node_waiters.lock().unwrap().insert(request_id, tx);

        let msg = MerkleTreeMessage::new(
            protocol_id,
            MerkleTreeMessageType::new(MerkleTreeMessageTypeValues::SyncNodeRequest),
            session_id.to_string(),
            MerkleTreeMessageWrapper::SyncNodeRequest(request_id, node_ids),
        );
        let _ = self
            .state
            .send_through_socket(
                self.identifier.connection_info().clone(),
                Box::new(neighbor.clone()),
                Box::new(msg),
            )
            .await;

        match timeout(RESPONSE_TIMEOUT, rx).await {
            Ok(Ok(answers)) => answers,
            _ => {
                self.node_waiters.lock().unwrap().remove(&request_id);
                error!("Timed out waiting for SyncNodeResponse from {:?}", neighbor);
                Vec::new()
            }
        }
    }

    /// Send one batched DataRequest and await the matching response.
    async fn request_data(
        &self,
        protocol_id: Option<u64>,
        neighbor: &NodeAddress,
        session_id: &str,
        keys: Vec<String>,
    ) -> Vec<(String, String)> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.data_waiters.lock().unwrap().insert(request_id, tx);

        let msg = MerkleTreeMessage::new(
            protocol_id,
            MerkleTreeMessageType::new(MerkleTreeMessageTypeValues::DataRequest),
            session_id.to_string(),
            MerkleTreeMessageWrapper::DataRequest(request_id, keys),
        );
        let _ = self
            .state
            .send_through_socket(
                self.identifier.connection_info().clone(),
                Box::new(neighbor.clone()),
                Box::new(msg),
            )
            .await;

        match timeout(RESPONSE_TIMEOUT, rx).await {
            Ok(Ok(pairs)) => pairs,
            _ => {
                self.data_waiters.lock().unwrap().remove(&request_id);
                error!("Timed out waiting for DataResponse from {:?}", neighbor);
                Vec::new()
            }
        }
    }

    /// Store the fetched key/values (last-writer-wins) and rebuild the tree once.
    async fn apply_fetched(&self, fetched: Vec<(String, String)>) {
        let Some(storage) = self.state.get_storage("default".to_string()) else {
            return;
        };
        // Snapshot the current contents once. Calling storage.get() per key would
        // reload the whole persistent file on every cache miss (and every fetched
        // key is a miss here), so resolve last-writer-wins against this map.
        let mut current: HashMap<String, String> = storage
            .items()
            .into_iter()
            .map(|i| (i.key().to_string(), i.value().to_string()))
            .collect();

        for (key, value) in fetched {
            let should_store = match current.get(&key) {
                Some(existing) => &value > existing,
                None => true,
            };
            if should_store {
                let item = Box::new(state::storage::item::DefaultDataStateItem::new(
                    key.clone(),
                    value.clone(),
                ));
                // store_silent avoids firing the Insert listener (which rebuilds
                // the whole tree per item); the tree is rebuilt once below.
                storage.store_silent(item).await;
                current.insert(key, value);
            }
        }

        let entries: Vec<(String, String)> = current.into_iter().collect();
        self.tree.replace_all(entries);
    }

    fn record_completion(&self, neighbor: &NodeAddress, duration: f64) {
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

    /// Responder side: answer a batch of node-ids from a session-stable snapshot.
    fn handle_sync_node_request(
        self: &Arc<Self>,
        protocol_id: Option<u64>,
        neighbor: NodeAddress,
        request_id: u64,
        node_ids: Vec<String>,
        session_id: String,
    ) {
        counter!("merkle_tree_sync_node_request_received", "neighbor" => format!("{:?}", neighbor))
            .increment(1);

        // Freeze a snapshot on the first request for this session.
        {
            let mut snaps = self.remote_snapshots.lock().unwrap();
            if !snaps.contains_key(&session_id) {
                snaps.insert(session_id.clone(), self.tree.snapshot());
            }
        }

        let answers: Vec<MerkleNodeAnswer> = {
            let snaps = self.remote_snapshots.lock().unwrap();
            let snap = &snaps[&session_id];
            node_ids
                .into_iter()
                .map(|id| match snap.get_node_summary(&id) {
                    Some((hash, key)) => (id, hash, key),
                    None => (id, [0; 32], None),
                })
                .collect()
        };

        let state_clone = self.state.clone();
        let id_clone = self.identifier.connection_info().clone();
        spawn!({
            let response = MerkleTreeMessage::new(
                protocol_id,
                MerkleTreeMessageType::new(MerkleTreeMessageTypeValues::SyncNodeResponse),
                session_id,
                MerkleTreeMessageWrapper::SyncNodeResponse(request_id, answers),
            );
            let _ = state_clone
                .send_through_socket(id_clone, Box::new(neighbor), Box::new(response))
                .await;
        });
    }

    /// Responder side: return the values for a batch of requested keys.
    fn handle_data_request(
        self: &Arc<Self>,
        protocol_id: Option<u64>,
        neighbor: NodeAddress,
        request_id: u64,
        keys: Vec<String>,
        session_id: String,
    ) {
        counter!("merkle_tree_data_request_received", "neighbor" => format!("{:?}", neighbor))
            .increment(1);

        let pairs: Vec<(String, String)> = {
            let data = self.tree.data.read().unwrap();
            keys.into_iter()
                .filter_map(|k| data.get(&k).map(|v| (k, v.clone())))
                .collect()
        };

        let state_clone = self.state.clone();
        let id_clone = self.identifier.connection_info().clone();
        spawn!({
            let response = MerkleTreeMessage::new(
                protocol_id,
                MerkleTreeMessageType::new(MerkleTreeMessageTypeValues::DataResponse),
                session_id,
                MerkleTreeMessageWrapper::DataResponse(request_id, pairs),
            );
            let _ = state_clone
                .send_through_socket(id_clone, Box::new(neighbor), Box::new(response))
                .await;
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
