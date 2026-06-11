pub mod deserializer;
pub mod messages;
pub mod protocols;
pub mod receiver;
pub mod scom;

use crate::algorithms::rbf::bloom::BloomFilter;

use std::sync::Arc;
use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

use connection::node::port::NodeAddress;
use state::node::{DefaultNodeState, NodeState};
use tokio::sync::{Notify, RwLock};

use crate::rbf_riblt::deserializer::RbfRibltDeserializer;
use crate::rbf_riblt::scom::{RbfScomSink, RbfScomTransport};
use crate::riblt_core::stream::RibltStreamEngine;

pub const RBF_RIBLT_PROTOCOL_ID: u64 = protocol::ProtocolId::RbfRiblt as u64;

pub const BLOOM_HASHES: u64 = 1;
pub const BLOOM_C_ELEM: usize = 32;

pub struct BloomSendingState {
    pub session_id: String,
    pub next_slice_index: u64,
    pub m_bits: usize,
    // Highest slice count the receiver has confirmed processing. The sender keeps
    // at most BLOOM_SEND_WINDOW slices in flight (next_slice_index - acked) so it
    // can't outrun the O(n)-per-slice receiver and flood it with unprocessed
    // slices. Woken via ack_notify on each ack and on the stop signal.
    pub acked: u64,
    pub ack_notify: Arc<Notify>,
}

impl BloomSendingState {
    pub fn new(session_id: String, m_bits: usize) -> Self {
        Self {
            session_id,
            next_slice_index: 0,
            m_bits,
            acked: 0,
            ack_notify: Arc::new(Notify::new()),
        }
    }
}

pub struct BloomReceivingState {
    pub session_id: String,
    pub filters: Vec<BloomFilter<String>>,
    pub m_bits: usize,
    pub s_com: Vec<String>,
    pub s_tn: Vec<String>,
    pub riblt_started: bool,
    // Number of bloom slices applied to this peer's stream before stabilization
    // (the `S` in the global FPR (1/2)^S). Recorded as a metric alongside
    // |s_com| so the analysis can relate slice count and candidate-set size to
    // similarity.
    pub slices_received: usize,
}

impl BloomReceivingState {
    /// Cost-based stopping criterion (Gomes & Baquero): stop slicing once a new
    /// slice reveals fewer than `m_bits / BLOOM_C_ELEM` new true negatives —
    /// i.e. the m-bit cost of another slice exceeds the cost of reconciling the
    /// elements it would still eliminate. Compared in integer form
    /// (`new_true_negatives * BLOOM_C_ELEM < m_bits`) to avoid truncating the
    /// threshold to zero for small filters.
    pub fn should_stop_slicing(&self, new_true_negatives: usize) -> bool {
        new_true_negatives * BLOOM_C_ELEM < self.m_bits
    }

    pub fn new(session_id: String, m_bits: usize) -> Self {
        Self {
            session_id,
            filters: Vec::new(),
            m_bits,
            s_com: Vec::new(),
            s_tn: Vec::new(),
            riblt_started: false,
            slices_received: 0,
        }
    }
}

pub struct RbfRibltProtocol {
    pub(crate) state: Arc<DefaultNodeState>,
    pub(crate) port: NodeAddress,
    pub(crate) deserializer: Arc<RbfRibltDeserializer>,
    pub(crate) bloom_sending_states: Arc<RwLock<HashMap<NodeAddress, BloomSendingState>>>,
    pub(crate) bloom_receiving_states: Arc<RwLock<HashMap<NodeAddress, BloomReceivingState>>>,
    // The scom phase reconciles the post-bloom s_com subsets through the shared
    // rateless-IBLT streaming engine (see `scom`).
    pub(crate) scom_engine: Arc<RibltStreamEngine>,
    pub(crate) pending_value_fetch_sessions: Arc<RwLock<HashMap<NodeAddress, String>>>,
    pub(crate) last_reconciled_fingerprint: Arc<RwLock<HashMap<NodeAddress, u64>>>,
    pub(crate) reconciliation_initiated_with: Arc<RwLock<HashSet<NodeAddress>>>,
    // Reconciliation start timestamps, stamped when the initiator begins the bloom
    // phase (and as a fallback when the scom decoder is seeded). Read by
    // handle_value_fetch_response to compute the whole-reconciliation duration,
    // and outlives the per-phase state that is torn down before the metric fires.
    pub(crate) round_start_times: Arc<RwLock<HashMap<NodeAddress, Instant>>>,
    // s_tn captured at bloom stabilization so handle_value_fetch_request can still access it
    // even after clear_session_state has wiped bloom_receiving_states.
    pub(crate) captured_stn: Arc<RwLock<HashMap<NodeAddress, Vec<String>>>>,
}

impl RbfRibltProtocol {
    pub fn new(state: Arc<DefaultNodeState>, port: NodeAddress) -> Self {
        // Maps shared between the protocol, the scom sink, and the value-fetch
        // handlers must be the same Arc instances.
        let bloom_receiving_states = Arc::new(RwLock::new(HashMap::new()));
        let pending_value_fetch_sessions = Arc::new(RwLock::new(HashMap::new()));
        let round_start_times = Arc::new(RwLock::new(HashMap::new()));

        let own_id = state.node_identifier().connection_info().clone();
        let transport = Arc::new(RbfScomTransport {
            state: state.clone(),
            own_id: own_id.clone(),
        });
        let sink = Arc::new(RbfScomSink {
            state: state.clone(),
            own_id,
            bloom_receiving_states: bloom_receiving_states.clone(),
            pending_value_fetch_sessions: pending_value_fetch_sessions.clone(),
            round_start_times: round_start_times.clone(),
            pending_remote: Arc::new(RwLock::new(HashMap::new())),
        });
        let scom_engine = Arc::new(RibltStreamEngine::new(transport, sink));

        Self {
            state,
            port,
            deserializer: Arc::new(RbfRibltDeserializer::new()),
            bloom_sending_states: Arc::new(RwLock::new(HashMap::new())),
            bloom_receiving_states,
            scom_engine,
            pending_value_fetch_sessions,
            last_reconciled_fingerprint: Arc::new(RwLock::new(HashMap::new())),
            reconciliation_initiated_with: Arc::new(RwLock::new(HashSet::new())),
            round_start_times,
            captured_stn: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}
