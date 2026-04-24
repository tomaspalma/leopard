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
    pub start_time: Instant,
}

pub struct SComReceivingState {
    pub local_iblt: RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>>,
    pub remote_iblt: UnmanagedRatelessIBLT<RIBLTSymbol>,
    pub session_id: String,
    pub start_time: Instant,
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

pub struct BloomReceivingState {
    pub session_id: String,
    pub filters: Vec<BloomFilter<String>>,
    pub m_bits: usize,
    pub s_com: Vec<String>,
    pub s_tn: Vec<String>,
    pub last_true_negatives: usize,
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
        }
    }
}
