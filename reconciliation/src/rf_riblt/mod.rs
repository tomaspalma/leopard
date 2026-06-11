pub mod deserializer;
pub mod messages;
pub mod protocols;
pub mod receiver;

use std::{
    collections::{HashMap, HashSet},
    hash::BuildHasherDefault,
    sync::Arc,
    time::Instant,
};

use connection::node::port::NodeAddress;
use crate::algorithms::ribbon::RibbonFilter;
use riblt::{Decoder, RatelessIBLT};
use state::node::DefaultNodeState;
use tokio::sync::RwLock;

use crate::rf_riblt::deserializer::RfRibltDeserializer;
use crate::riblt_core::RIBLTSymbol;

pub const RF_RIBLT_PROTOCOL_ID: u64 = protocol::ProtocolId::RfRiblt as u64;

pub const RF_FINGERPRINT_BITS: u32 = 8;
pub const RF_RIBBON_WIDTH: u32 = 64;
// ε ≈ (4 + r/4) / w = (4 + 2) / 64 ≈ 0.094 per the Ribbon Filter paper (§4).
// Homogeneous mode guarantees construction success so no retry margin needed.
pub const RF_OVERHEAD: f64 = 0.1;
pub const RIBLT_BATCH_SIZE: usize = 5;

pub type RfRibltHasher = BuildHasherDefault<std::collections::hash_map::DefaultHasher>;
pub type RfRibbonFilter = RibbonFilter<RfRibltHasher>;

/// Phase of the request/response-driven scom IBLT exchange. Unlike riblt and
/// rbf_riblt, which stream symbols continuously under a credit window (see
/// `riblt_core::stream`), rf_riblt sends a batch and waits for confirmation.
#[derive(Debug, Clone, PartialEq)]
pub enum SComReconciliationState {
    SendingSymbols,
    AwaitingConfirmation,
}

pub struct SComSendingState {
    pub state: SComReconciliationState,
    pub local_iblt: RatelessIBLT<RIBLTSymbol>,
    pub session_id: String,
}

pub struct SComReceivingState {
    pub decoder: Decoder<RIBLTSymbol>,
    pub session_id: String,
}

pub struct RfFilterReceivingState {
    pub session_id: String,
    pub s_com: Vec<String>,
    pub s_tn: Vec<String>,
    pub riblt_started: bool,
}

impl RfFilterReceivingState {
    pub fn new(session_id: String) -> Self {
        Self {
            session_id,
            s_com: Vec::new(),
            s_tn: Vec::new(),
            riblt_started: false,
        }
    }
}

pub struct RfRibltProtocol {
    pub(crate) state: Arc<DefaultNodeState>,
    pub(crate) port: NodeAddress,
    pub(crate) deserializer: Arc<RfRibltDeserializer>,
    pub(crate) filter_receiving_states: Arc<RwLock<HashMap<NodeAddress, RfFilterReceivingState>>>,
    pub(crate) scom_sending_states: Arc<RwLock<HashMap<NodeAddress, SComSendingState>>>,
    pub(crate) scom_receiving_states: Arc<RwLock<HashMap<NodeAddress, SComReceivingState>>>,
    pub(crate) pending_value_fetch_sessions: Arc<RwLock<HashMap<NodeAddress, String>>>,
    pub(crate) last_reconciled_fingerprint: Arc<RwLock<HashMap<NodeAddress, u64>>>,
    pub(crate) reconciliation_initiated_with: Arc<RwLock<HashSet<NodeAddress>>>,
    pub(crate) round_start_times: Arc<RwLock<HashMap<NodeAddress, Instant>>>,
    pub(crate) captured_stn: Arc<RwLock<HashMap<NodeAddress, Vec<String>>>>,
}

impl RfRibltProtocol {
    pub fn new(state: Arc<DefaultNodeState>, port: NodeAddress) -> Self {
        Self {
            state,
            port,
            deserializer: Arc::new(RfRibltDeserializer::new()),
            filter_receiving_states: Arc::new(RwLock::new(HashMap::new())),
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
