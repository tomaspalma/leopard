pub mod bloom;
pub mod deserializer;
pub mod messages;
pub mod protocols;
pub mod rateless_bloom;
pub mod receiver;

use bloom::BloomFilter;

use std::sync::Arc;
use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

use connection::node::port::NodeAddress;
use riblt::{RatelessIBLT, UnmanagedRatelessIBLT};
use state::node::DefaultNodeState;
use tokio::sync::RwLock;

use crate::rbf_riblt::deserializer::RbfRibltDeserializer;
use crate::riblt::messages::RIBLTSymbol;

pub const RBF_RIBLT_PROTOCOL_ID: u64 = 3;

pub const BLOOM_HASHES: u64 = 1;
pub const BLOOM_C_ELEM: usize = 32;
pub const RIBLT_BATCH_SIZE: usize = 5;

#[derive(Debug, Clone, PartialEq)]
pub enum SComReconciliationState {
    SendingSymbols,
    AwaitingConfirmation,
}

pub struct SComSendingState {
    pub state: SComReconciliationState,
    pub local_iblt: RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>>,
    pub session_id: String,
}

pub struct SComReceivingState {
    pub local_iblt: RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>>,
    pub remote_iblt: UnmanagedRatelessIBLT<RIBLTSymbol>,
    pub session_id: String,
}

pub struct BloomSendingState {
    pub session_id: String,
    pub next_slice_index: u64,
    pub m_bits: usize,
}

impl BloomSendingState {
    pub fn new(session_id: String, m_bits: usize) -> Self {
        Self {
            session_id,
            next_slice_index: 0,
            m_bits,
        }
    }
}

pub const STABLE_ROUNDS_REQUIRED: usize = 3;

pub struct BloomReceivingState {
    pub session_id: String,
    pub filters: Vec<BloomFilter<String>>,
    pub m_bits: usize,
    pub s_com: Vec<String>,
    pub s_tn: Vec<String>,
    pub last_true_negatives: usize,
    pub consecutive_stable_rounds: usize,
    pub riblt_started: bool,
}

impl BloomReceivingState {
    pub fn new(session_id: String, m_bits: usize) -> Self {
        Self {
            session_id,
            filters: Vec::new(),
            m_bits,
            s_com: Vec::new(),
            s_tn: Vec::new(),
            last_true_negatives: 0,
            consecutive_stable_rounds: 0,
            riblt_started: false,
        }
    }
}

pub struct RbfRibltProtocol {
    pub(crate) state: Arc<DefaultNodeState>,
    pub(crate) port: NodeAddress,
    pub(crate) deserializer: Arc<RbfRibltDeserializer>,
    pub(crate) bloom_sending_states: Arc<RwLock<HashMap<NodeAddress, BloomSendingState>>>,
    pub(crate) bloom_receiving_states: Arc<RwLock<HashMap<NodeAddress, BloomReceivingState>>>,
    pub(crate) scom_sending_states: Arc<RwLock<HashMap<NodeAddress, SComSendingState>>>,
    pub(crate) scom_receiving_states: Arc<RwLock<HashMap<NodeAddress, SComReceivingState>>>,
    pub(crate) pending_value_fetch_sessions: Arc<RwLock<HashMap<NodeAddress, String>>>,
    pub(crate) last_reconciled_fingerprint: Arc<RwLock<HashMap<NodeAddress, u64>>>,
    pub(crate) reconciliation_initiated_with: Arc<RwLock<HashSet<NodeAddress>>>,
    // Preserved from scom_receiving_states before it is removed, so handle_value_fetch_response
    // can still compute the round duration after that state is gone.
    pub(crate) round_start_times: Arc<RwLock<HashMap<NodeAddress, Instant>>>,
    // s_tn captured at bloom stabilization so handle_value_fetch_request can still access it
    // even after clear_session_state has wiped bloom_receiving_states.
    pub(crate) captured_stn: Arc<RwLock<HashMap<NodeAddress, Vec<String>>>>,
}

impl RbfRibltProtocol {
    pub fn new(state: Arc<DefaultNodeState>, port: NodeAddress) -> Self {
        Self {
            state,
            port,
            deserializer: Arc::new(RbfRibltDeserializer::new()),
            bloom_sending_states: Arc::new(RwLock::new(HashMap::new())),
            bloom_receiving_states: Arc::new(RwLock::new(HashMap::new())),
            scom_sending_states: Arc::new(RwLock::new(HashMap::new())),
            scom_receiving_states: Arc::new(RwLock::new(HashMap::new())),
            pending_value_fetch_sessions: Arc::new(RwLock::new(HashMap::new())),
            last_reconciled_fingerprint: Arc::new(RwLock::new(HashMap::new())),
            reconciliation_initiated_with: Arc::new(RwLock::new(HashSet::new())),
            round_start_times: Arc::new(RwLock::new(HashMap::new())),
            captured_stn: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}
