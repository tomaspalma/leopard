pub mod bloom;
pub mod deserializer;
pub mod messages;
pub mod protocols;
pub mod receiver;

use bloom::BloomFilter;

use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    hash::Hash,
    mem,
    time::{Duration, Instant},
};
use std::sync::Arc;

use connection::node::port::NodeAddress;
use riblt::{RatelessIBLT, UnmanagedRatelessIBLT};
use state::node::DefaultNodeState;
use tokio::sync::RwLock;

use crate::rbf_riblt::deserializer::RbfRibltDeserializer;
use crate::riblt::messages::RIBLTSymbol;

pub trait StoppingStrategyFactory<T: Hash> {
    type Strategy: StoppingStrategy<T>;
    fn create(&self, elements: Vec<T>, sample_size: usize) -> Self::Strategy;
    fn print_name(&self) -> String;
    fn print_params(&self) -> String;
}

pub trait StoppingStrategy<T: Hash> {
    fn on_extend(&mut self, bf: &mut RatelessBF<T>);
    fn should_stop(&mut self, bf: &mut RatelessBF<T>) -> Option<(Vec<T>, Vec<T>)>;
}

pub struct RatelessBF<T: Hash> {
    bloom_filters: Vec<BloomFilter<T>>,
    data: Vec<T>,
    m: usize,
    t_enc: Duration,
    t_dec: Duration,
}

impl<T> RatelessBF<T>
where
    T: Hash,
{
    #[inline]
    #[must_use]
    pub fn new(data: Vec<T>, m: usize) -> Self {
        Self {
            bloom_filters: Vec::new(),
            data,
            m: max(m, 1),
            t_enc: Duration::from_secs(0),
            t_dec: Duration::from_secs(0),
        }
    }

    pub fn extend(&mut self) {
        let mut filter = BloomFilter::from_raw_parts(self.m, 1);
        self.data.iter().for_each(|d| filter.insert(d));
        self.bloom_filters.push(filter);
    }

    pub fn extend_with_seeds(&mut self, seeds: [u64; 2]) {
        let mut filter = BloomFilter::from_raw_parts_with_seeds(self.m, 1, seeds);
        self.data.iter().for_each(|d| filter.insert(d));
        self.bloom_filters.push(filter);
    }

    pub fn filter_count(&self) -> usize {
        self.bloom_filters.len()
    }

    pub fn latest_filter(&self) -> Option<&BloomFilter<T>> {
        self.bloom_filters.last()
    }

    pub fn contains(&self, value: &T) -> bool {
        self.bloom_filters
            .iter()
            .all(|filter| filter.contains(value))
    }

    pub fn extend_until<S: StoppingStrategy<T>>(&mut self, mut strategy: S) -> (Vec<T>, Vec<T>) {
        let mut _run = 1;
        loop {
            let exec_time = Instant::now();
            self.extend();
            self.t_enc += exec_time.elapsed();
            let exec_time = Instant::now();
            strategy.on_extend(self);
            if let Some(partitioned_elements) = strategy.should_stop(self) {
                self.t_dec += exec_time.elapsed();
                return partitioned_elements;
            }
            self.t_dec += exec_time.elapsed();
            _run += 1;
        }
    }

    pub fn on_extend<S: StoppingStrategy<T>>(&mut self, mut strategy: S) {
        strategy.on_extend(self);
    }

    pub fn size_of(&self) -> usize {
        if self.bloom_filters.is_empty() {
            return 0;
        }

        let standalone_bf = &self.bloom_filters[0];
        let standalone_bf_size = standalone_bf.bitslice().chunks(8).count();

        self.bloom_filters.len() * standalone_bf_size + mem::size_of::<u64>()
    }

    #[inline]
    pub fn t_enc(&self) -> Duration {
        self.t_enc
    }

    #[inline]
    pub fn t_dec(&self) -> Duration {
        self.t_dec
    }
}

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
