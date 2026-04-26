use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    sync::Arc,
    time::Instant,
};

use connection::{node::port::NodeAddress, route::RouteTask};
use metrics::{counter, gauge, histogram};
use protocol::deserializer::ProtocolDeserializer;
use riblt::{symbol::PeelableResult, RatelessIBLT, UnmanagedRatelessIBLT};
use runtime::metrics::experiment::get_context;
use runtime::spawn;
use state::node::NodeState;
use tracing::{error, info};

use crate::rbf_riblt::{
    bloom::BloomFilter,
    deserializer::RbfRibltDeserializer,
    messages::{
        RbfRibltBloomFilterSliceMessage, RbfRibltFetchedEntry, RbfRibltHandshakeMessage,
        RbfRibltMessageTypeValues, RbfRibltRBFStopSignalMessage, RbfRibltSComDecodedAllMessage,
        RbfRibltSComRequestMoreSymbolsMessage, RbfRibltSComSendSymbolMessage,
        RbfRibltValueFetchRequestMessage, RbfRibltValueFetchResponseMessage,
    },
    BloomReceivingState, BloomSendingState, SComReceivingState, SComReconciliationState,
    SComSendingState, RBF_RIBLT_PROTOCOL_ID,
};
use crate::riblt::messages::RIBLTSymbol;

use super::RbfRibltProtocol;

fn hash_storage_pairs(pairs: &[(String, String)]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    pairs.len().hash(&mut hasher);
    for (k, v) in pairs {
        k.hash(&mut hasher);
        v.hash(&mut hasher);
    }
    hasher.finish()
}

impl RbfRibltProtocol {
    /// True if any reconciliation state exists for this neighbor — used to
    /// guard against starting a new session while one is already running.
    pub async fn is_session_active(&self, neighbor: &NodeAddress) -> bool {
        self.bloom_sending_states
            .read()
            .await
            .contains_key(neighbor)
            || self
                .bloom_receiving_states
                .read()
                .await
                .contains_key(neighbor)
            || self.scom_sending_states.read().await.contains_key(neighbor)
            || self
                .scom_receiving_states
                .read()
                .await
                .contains_key(neighbor)
            || self
                .pending_value_fetch_sessions
                .read()
                .await
                .contains_key(neighbor)
    }

    /// True if the session is past the bloom phase (scom or fetch in progress),
    /// or if bloom has already transitioned to scom. Used by handshake handling
    /// to allow bloom-phase resets while blocking scom/fetch-phase interference.
    pub async fn is_session_busy(&self, neighbor: &NodeAddress) -> bool {
        self.scom_sending_states.read().await.contains_key(neighbor)
            || self
                .scom_receiving_states
                .read()
                .await
                .contains_key(neighbor)
            || self
                .pending_value_fetch_sessions
                .read()
                .await
                .contains_key(neighbor)
            || self
                .bloom_receiving_states
                .read()
                .await
                .get(neighbor)
                .map(|s| s.riblt_started)
                .unwrap_or(false)
    }

    pub async fn clear_session_state(&self, neighbor: &NodeAddress) {
        self.pending_value_fetch_sessions
            .write()
            .await
            .remove(neighbor);
        self.scom_sending_states.write().await.remove(neighbor);
        self.scom_receiving_states.write().await.remove(neighbor);
        self.round_start_times.write().await.remove(neighbor);
        self.bloom_sending_states.write().await.remove(neighbor);
        self.bloom_receiving_states.write().await.remove(neighbor);
        self.captured_stn.write().await.remove(neighbor);
        self.reconciliation_initiated_with
            .write()
            .await
            .remove(neighbor);
    }

    pub async fn update_last_reconciled_fingerprint(&self, neighbor: &NodeAddress) {
        let Some(storage) = self.state.get_storage("default".to_string()) else {
            return;
        };
        let mut pairs: Vec<(String, String)> = storage
            .items()
            .into_iter()
            .map(|item| (item.key().to_string(), item.value().to_string()))
            .collect();
        pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        self.last_reconciled_fingerprint
            .write()
            .await
            .insert(neighbor.clone(), hash_storage_pairs(&pairs));
    }
}

pub struct ReceiveRbfRibltMessageTask {
    protocol: Arc<RbfRibltProtocol>,
}

impl ReceiveRbfRibltMessageTask {
    pub fn new(protocol: Arc<RbfRibltProtocol>) -> Self {
        Self { protocol }
    }

    fn compute_local_fingerprint(&self) -> Option<u64> {
        let storage = self.protocol.state.get_storage("default".to_string())?;
        let mut pairs: Vec<(String, String)> = storage
            .items()
            .into_iter()
            .map(|item| (item.key().to_string(), item.value().to_string()))
            .collect();
        pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        Some(hash_storage_pairs(&pairs))
    }

    async fn process_handshake(&self, msg: &RbfRibltHandshakeMessage, neighbor: NodeAddress) {
        if self.protocol.is_session_busy(&neighbor).await {
            return;
        }

        if let Some(fp) = self.compute_local_fingerprint() {
            let last_fp = self
                .protocol
                .last_reconciled_fingerprint
                .read()
                .await
                .get(&neighbor)
                .copied();
            if last_fp == Some(fp) {
                return;
            }
        }

        let storage = match self.protocol.state.get_storage("default".to_string()) {
            Some(s) => s,
            None => return,
        };

        let local_size = storage.items().len();
        let m_bits = ((local_size.max(1)) as f64 / std::f64::consts::LN_2).ceil() as usize;

        let mut receiving = self.protocol.bloom_receiving_states.write().await;
        let should_reset = receiving
            .get(&neighbor)
            .map(|st| st.session_id != msg.session_id())
            .unwrap_or(true);
        if should_reset {
            receiving.insert(
                neighbor.clone(),
                BloomReceivingState::new(msg.session_id().to_string(), m_bits),
            );
            self.protocol
                .bloom_sending_states
                .write()
                .await
                .remove(&neighbor);
        }

        info!(
            "Initialized bloom receiving state for {:?} session_id={} m_bits={}",
            neighbor,
            msg.session_id(),
            m_bits
        );
    }

    async fn init_bloom_receiving_state_if_needed(
        &self,
        msg: &RbfRibltBloomFilterSliceMessage,
        neighbor: &NodeAddress,
    ) -> bool {
        let mut receiving = self.protocol.bloom_receiving_states.write().await;
        let needs_init = receiving
            .get(neighbor)
            .map(|s| s.session_id != msg.session_id())
            .unwrap_or(true);

        if !needs_init {
            return true;
        }

        let storage = match self.protocol.state.get_storage("default".to_string()) {
            Some(s) => s,
            None => return false,
        };

        let mut state = BloomReceivingState::new(msg.session_id().to_string(), msg.m());
        state.s_com = storage
            .items()
            .into_iter()
            .map(|item| item.key().to_string())
            .collect();
        receiving.insert(neighbor.clone(), state);
        true
    }

    fn partition_s_com(
        s_com: &[String],
        s_tn: &[String],
        filter: &BloomFilter<String>,
    ) -> (Vec<String>, Vec<String>) {
        let mut new_s_com = Vec::new();
        let mut new_s_tn = s_tn.to_vec();

        for key in s_com {
            if filter.contains(key) {
                new_s_com.push(key.clone());
            } else {
                new_s_tn.push(key.clone());
            }
        }

        (new_s_com, new_s_tn)
    }

    async fn apply_partition_to_state(
        &self,
        neighbor: &NodeAddress,
        new_s_com: Vec<String>,
        new_s_tn: Vec<String>,
    ) -> Option<(bool, String)> {
        let mut receiving = self.protocol.bloom_receiving_states.write().await;
        let state = receiving.get_mut(neighbor)?;
        if new_s_tn.len() == state.last_true_negatives {
            state.consecutive_stable_rounds += 1;
        } else {
            state.consecutive_stable_rounds = 0;
        }
        let stabilized =
            state.consecutive_stable_rounds >= crate::rbf_riblt::STABLE_ROUNDS_REQUIRED;
        state.last_true_negatives = new_s_tn.len();
        state.s_com = new_s_com;
        state.s_tn = new_s_tn;
        Some((stabilized, state.session_id.clone()))
    }

    async fn send_stop_signal(&self, neighbor: NodeAddress, session_id: String) {
        let _ = self
            .protocol
            .state
            .send_through_socket(
                self.protocol
                    .state
                    .node_identifier()
                    .connection_info()
                    .clone(),
                Box::new(neighbor),
                Box::new(RbfRibltRBFStopSignalMessage::new(
                    Some(RBF_RIBLT_PROTOCOL_ID),
                    session_id,
                )),
            )
            .await;
    }

    async fn start_scom_phase(&self, neighbor: NodeAddress) {
        info!("Starting scom phase");
        // Atomically check+set riblt_started to prevent duplicate starts
        // from in-flight bloom slices arriving after stabilization.
        let (s_com, s_tn) = {
            let mut receiving = self.protocol.bloom_receiving_states.write().await;
            match receiving.get_mut(&neighbor) {
                Some(state) if !state.riblt_started => {
                    state.riblt_started = true;
                    (state.s_com.clone(), state.s_tn.clone())
                }
                _ => return,
            }
        };

        // Capture s_tn now so handle_value_fetch_request can use it even after
        // clear_session_state wipes bloom_receiving_states.
        self.protocol
            .captured_stn
            .write()
            .await
            .insert(neighbor.clone(), s_tn);

        let storage = match self.protocol.state.get_storage("default".to_string()) {
            Some(s) => s,
            None => return,
        };

        let mut symbols = HashSet::new();
        for key in s_com {
            let value = storage
                .get(&key)
                .await
                .map(|item| item.value().to_string())
                .unwrap_or_default();
            symbols.insert(RIBLTSymbol { key, value });
        }

        let session_id = uuid::Uuid::new_v4().to_string();

        self.protocol.scom_sending_states.write().await.insert(
            neighbor.clone(),
            SComSendingState {
                state: SComReconciliationState::SendingSymbols,
                local_iblt: RatelessIBLT::new(symbols),
                session_id,
            },
        );

        let state = self.protocol.state.clone();
        let scom_sending_states = self.protocol.scom_sending_states.clone();

        spawn!({
            RbfRibltProtocol::stream_scom_symbols_to_neighbor(state, scom_sending_states, neighbor)
                .await;
        });
    }

    async fn start_reverse_stream(&self, neighbor: NodeAddress) {
        info!("Starting reverse stream phase");
        let (s_com, s_tn) = {
            let receiving = self.protocol.bloom_receiving_states.read().await;
            match receiving.get(&neighbor) {
                Some(s) => (s.s_com.clone(), s.s_tn.clone()),
                None => return,
            }
        };

        // Capture s_tn now so handle_value_fetch_request can use it even after
        // clear_session_state wipes bloom_receiving_states.
        self.protocol
            .captured_stn
            .write()
            .await
            .insert(neighbor.clone(), s_tn);

        let m_bits = ((s_com.len().max(1)) as f64 / std::f64::consts::LN_2).ceil() as usize;
        let session_id = uuid::Uuid::new_v4().to_string();

        // Guard: don't start a second reverse stream if one is already running.
        {
            let mut sending = self.protocol.bloom_sending_states.write().await;
            if sending.contains_key(&neighbor) {
                return;
            }
            sending.insert(neighbor.clone(), BloomSendingState::new(session_id, m_bits));
        }

        let state = self.protocol.state.clone();
        let bloom_sending_states = self.protocol.bloom_sending_states.clone();

        spawn!({
            RbfRibltProtocol::stream_fixed_slices_to_neighbor(
                state,
                bloom_sending_states,
                neighbor,
                s_com,
            )
            .await;
        });
    }

    async fn process_bloom_slice(
        &self,
        msg: &RbfRibltBloomFilterSliceMessage,
        neighbor: NodeAddress,
    ) {
        if !self
            .init_bloom_receiving_state_if_needed(msg, &neighbor)
            .await
        {
            return;
        }

        let filter =
            BloomFilter::<String>::from_raw_bits(msg.m(), msg.k(), msg.bits(), msg.seeds());

        let (new_s_com, new_s_tn) = {
            let receiving = self.protocol.bloom_receiving_states.read().await;
            let state = match receiving.get(&neighbor) {
                Some(s) => s,
                None => return,
            };
            Self::partition_s_com(&state.s_com, &state.s_tn, &filter)
        };

        let (stabilized, session_id) = match self
            .apply_partition_to_state(&neighbor, new_s_com, new_s_tn)
            .await
        {
            Some(result) => result,
            None => return,
        };

        if stabilized {
            self.send_stop_signal(neighbor.clone(), session_id).await;

            let is_initiator = self
                .protocol
                .reconciliation_initiated_with
                .read()
                .await
                .contains(&neighbor);

            if is_initiator {
                self.start_scom_phase(neighbor).await;
            } else {
                self.start_reverse_stream(neighbor).await;
            }
        }
    }

    fn partition_peeled_symbols(
        peeled_symbols: Vec<PeelableResult<RIBLTSymbol>>,
    ) -> (Vec<RIBLTSymbol>, Vec<RIBLTSymbol>) {
        let mut remote = Vec::new();
        let mut local = Vec::new();
        for symbol in peeled_symbols {
            match symbol {
                PeelableResult::Remote(s) => remote.push(s),
                PeelableResult::Local(s) => local.push(s),
                _ => {}
            }
        }
        (remote, local)
    }

    async fn init_scom_receiving_state_if_needed(&self, neighbor: &NodeAddress, session_id: &str) {
        let should_reset = self
            .protocol
            .scom_receiving_states
            .read()
            .await
            .get(neighbor)
            .map(|s| s.session_id != session_id)
            .unwrap_or(false);

        if should_reset {
            self.protocol
                .scom_receiving_states
                .write()
                .await
                .remove(neighbor);
        }

        if self
            .protocol
            .scom_receiving_states
            .read()
            .await
            .contains_key(neighbor)
        {
            return;
        }

        let s_com: Vec<String> = self
            .protocol
            .bloom_receiving_states
            .read()
            .await
            .get(neighbor)
            .map(|state| state.s_com.clone())
            .unwrap_or_default();

        let storage = self.protocol.state.get_storage("default".to_string());
        let mut symbols = HashSet::new();
        for key in s_com {
            let value = if let Some(ref s) = storage {
                s.get(&key)
                    .await
                    .map(|item| item.value().to_string())
                    .unwrap_or_default()
            } else {
                String::new()
            };
            symbols.insert(RIBLTSymbol { key, value });
        }

        self.protocol
            .round_start_times
            .write()
            .await
            .insert(neighbor.clone(), Instant::now());

        self.protocol.scom_receiving_states.write().await.insert(
            neighbor.clone(),
            SComReceivingState {
                local_iblt: riblt::RatelessIBLT::new(symbols),
                remote_iblt: UnmanagedRatelessIBLT::new(),
                session_id: session_id.to_string(),
            },
        );
    }

    async fn handle_scom_send_symbols(
        &self,
        message: RbfRibltSComSendSymbolMessage,
        neighbor: NodeAddress,
    ) {
        self.init_scom_receiving_state_if_needed(&neighbor, message.session_id())
            .await;

        let (local_coded_symbols, remote_coded_symbols) = match self
            .protocol
            .scom_receiving_states
            .write()
            .await
            .get_mut(&neighbor)
        {
            Some(status) => {
                for symbol in message.symbols() {
                    let mut cs = riblt::CodedSymbol::new();
                    cs.sum = symbol.sum.clone();
                    cs.hash = symbol.hash;
                    cs.count = symbol.count;
                    status.remote_iblt.add_coded_symbol(&cs);
                }

                let coded_symbols_len = status.remote_iblt.coded_symbols.len();
                status.local_iblt.extend_coded_symbols(coded_symbols_len);

                (
                    status.local_iblt.coded_symbols.clone(),
                    status.remote_iblt.coded_symbols.clone(),
                )
            }
            None => return,
        };

        let neighbor_clone = neighbor.clone();
        let (is_peeling_successful, remote_symbols, local_symbols) =
            tokio::task::spawn_blocking(move || {
                let local_iblt = UnmanagedRatelessIBLT {
                    coded_symbols: local_coded_symbols,
                };
                let remote_iblt = UnmanagedRatelessIBLT {
                    coded_symbols: remote_coded_symbols,
                };

                let decode_start = std::time::Instant::now();
                let mut collapsed = local_iblt.collapse(&remote_iblt);
                let peel_symbols = collapsed.peel_all_symbols();
                let (remote, local) = Self::partition_peeled_symbols(peel_symbols);

                histogram!(
                    "rbf_riblt_decode_duration_seconds",
                    "neighbor" => format!("{:?}", neighbor_clone)
                )
                .record(decode_start.elapsed().as_secs_f64());

                let successful = collapsed.is_empty();
                (successful, remote, local)
            })
            .await
            .unwrap();

        if is_peeling_successful {
            // Keys the neighbor is missing: IBLT-local elements (we have, they don't) plus our
            // s_tn for this neighbor (keys definitely absent from their bloom filter).
            let mut keys_for_sender: Vec<String> =
                local_symbols.into_iter().map(|s| s.key).collect();
            let s_tn: Vec<String> = self
                .protocol
                .bloom_receiving_states
                .read()
                .await
                .get(&neighbor)
                .map(|s| s.s_tn.clone())
                .unwrap_or_default();
            keys_for_sender.extend(s_tn);

            let missing_keys: Vec<String> = remote_symbols.into_iter().map(|s| s.key).collect();

            self.protocol
                .scom_receiving_states
                .write()
                .await
                .remove(&neighbor);

            self.protocol
                .pending_value_fetch_sessions
                .write()
                .await
                .insert(neighbor.clone(), message.session_id().clone());

            let _ = self
                .protocol
                .state
                .send_through_socket(
                    self.protocol
                        .state
                        .node_identifier()
                        .connection_info()
                        .clone(),
                    Box::new(neighbor.clone()),
                    Box::new(RbfRibltSComDecodedAllMessage::new(
                        Some(RBF_RIBLT_PROTOCOL_ID),
                        message.session_id().clone(),
                        keys_for_sender,
                    )),
                )
                .await;

            // Always send a fetch request so the responder piggybacks their s_tn values,
            // even when missing_keys is empty.
            let _ = self
                .protocol
                .state
                .send_through_socket(
                    self.protocol
                        .state
                        .node_identifier()
                        .connection_info()
                        .clone(),
                    Box::new(neighbor),
                    Box::new(RbfRibltValueFetchRequestMessage::new(
                        Some(RBF_RIBLT_PROTOCOL_ID),
                        message.session_id().clone(),
                        missing_keys,
                    )),
                )
                .await;
        } else {
            let _ = self
                .protocol
                .state
                .send_through_socket(
                    self.protocol
                        .state
                        .node_identifier()
                        .connection_info()
                        .clone(),
                    Box::new(neighbor),
                    Box::new(RbfRibltSComRequestMoreSymbolsMessage::new(
                        Some(RBF_RIBLT_PROTOCOL_ID),
                        message.session_id().clone(),
                    )),
                )
                .await;
        }
    }

    async fn handle_scom_decoded_all(
        &self,
        message: RbfRibltSComDecodedAllMessage,
        neighbor: NodeAddress,
    ) {
        let should_remove = self
            .protocol
            .scom_sending_states
            .read()
            .await
            .get(&neighbor)
            .map(|state| state.session_id == *message.session_id())
            .unwrap_or(false);

        if should_remove {
            self.protocol
                .scom_sending_states
                .write()
                .await
                .remove(&neighbor);
            self.protocol
                .pending_value_fetch_sessions
                .write()
                .await
                .insert(neighbor.clone(), message.session_id().clone());

            // The IBLT receiver computed which keys we (the sender) are missing and included
            // them in the message. Always send the fetch request so the responder can also
            // piggyback their s_tn values back to us.
            let request_keys = message.keys_for_sender().clone();
            let _ = self
                .protocol
                .state
                .send_through_socket(
                    self.protocol
                        .state
                        .node_identifier()
                        .connection_info()
                        .clone(),
                    Box::new(neighbor),
                    Box::new(RbfRibltValueFetchRequestMessage::new(
                        Some(RBF_RIBLT_PROTOCOL_ID),
                        message.session_id().clone(),
                        request_keys,
                    )),
                )
                .await;
        }
    }

    async fn handle_scom_request_more(
        &self,
        message: RbfRibltSComRequestMoreSymbolsMessage,
        neighbor: NodeAddress,
    ) {
        if let Some(status) = self
            .protocol
            .scom_sending_states
            .write()
            .await
            .get_mut(&neighbor)
        {
            if status.session_id == *message.session_id() {
                status.state = SComReconciliationState::SendingSymbols;
            }
        }
    }

    async fn handle_value_fetch_request(
        &self,
        message: RbfRibltValueFetchRequestMessage,
        neighbor: NodeAddress,
    ) {
        let storage = match self.protocol.state.get_storage("default".to_string()) {
            Some(s) => s,
            None => return,
        };

        let wanted: HashSet<&str> = message.keys().iter().map(String::as_str).collect();

        // Piggyback our s_tn keys — these are keys we have that the neighbor definitely lacks
        // (they didn't pass the neighbor's bloom filter). The IBLT only reconciles s_com, so
        // these would never be discovered through IBLT decoding alone.
        // Read from captured_stn (frozen at bloom stabilization) rather than bloom_receiving_states,
        // which may already have been cleared by a concurrent handle_value_fetch_response.
        let s_tn_keys: HashSet<String> = self
            .protocol
            .captured_stn
            .read()
            .await
            .get(&neighbor)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default();

        let mut entries = Vec::new();
        for item in storage.items() {
            let key = item.key();
            if wanted.contains(key) || s_tn_keys.contains(key) {
                entries.push(RbfRibltFetchedEntry::new(
                    item.key().to_string(),
                    item.value().to_string(),
                ));
            }
        }

        let _ = self
            .protocol
            .state
            .send_through_socket(
                self.protocol
                    .state
                    .node_identifier()
                    .connection_info()
                    .clone(),
                Box::new(neighbor),
                Box::new(RbfRibltValueFetchResponseMessage::new(
                    Some(RBF_RIBLT_PROTOCOL_ID),
                    message.session_id().clone(),
                    entries,
                )),
            )
            .await;
    }

    async fn handle_value_fetch_response(
        &self,
        message: RbfRibltValueFetchResponseMessage,
        neighbor: NodeAddress,
    ) {
        let pending = self
            .protocol
            .pending_value_fetch_sessions
            .read()
            .await
            .get(&neighbor)
            .cloned();

        if pending.as_deref() != Some(message.session_id().as_str()) {
            return;
        }

        if let Some(storage) = self.protocol.state.get_storage("default".to_string()) {
            for entry in message.entries() {
                storage
                    .store(Box::new(state::storage::item::DefaultDataStateItem::new(
                        entry.key().to_string(),
                        entry.value().to_string(),
                    )))
                    .await;
            }
        }

        let differences_found = !message.entries().is_empty();

        let round_duration = self
            .protocol
            .round_start_times
            .read()
            .await
            .get(&neighbor)
            .map(|t| t.elapsed().as_secs_f64());

        self.protocol
            .update_last_reconciled_fingerprint(&neighbor)
            .await;
        self.protocol.clear_session_state(&neighbor).await;

        let context = get_context();

        if let Some(duration) = round_duration {
            gauge!(
                "reconciliation_round_duration_seconds",
                "protocol" => "rbf_riblt",
                "neighbor" => format!("{:?}", neighbor),
                "run_id" => context.run_id().to_string(),
                "trial" => context.trial().to_string(),
                "similarity" => context.similarity().to_string()
            )
            .set(duration);
        }

        counter!(
            "reconciliation_completed",
            "protocol" => "rbf_riblt",
            "neighbor" => format!("{:?}", neighbor),
            "run_id" => context.run_id().to_string(),
            "trial" => context.trial().to_string(),
            "similarity" => context.similarity().to_string()
        )
        .increment(1);

        runtime::metrics::csv::finish_iteration(
            format!(
                "{:?}",
                self.protocol.state.node_identifier().connection_info()
            ),
            format!("{:?}", neighbor),
            "rbf_riblt",
        );
    }
}

impl RouteTask for ReceiveRbfRibltMessageTask {
    fn run(self: Arc<Self>, message: Vec<u8>, neighbor: NodeAddress) {
        let deserialized_message = RbfRibltDeserializer::new().deserialize(message);
        let msg_type_box = deserialized_message.get_type().value();

        let msg_type = msg_type_box
            .as_any()
            .downcast_ref::<RbfRibltMessageTypeValues>()
            .cloned();

        let this = self.clone();
        spawn!({
            if let Some(msg_enum) = msg_type {
                let context = get_context();
                counter!(
                    "protocol_round_trip_count",
                    "target" => format!("{:?}", neighbor),
                    "protocol" => "rbf_riblt",
                    "run_id" => context.run_id().to_string(),
                    "trial" => context.trial().to_string(),
                    "similarity" => context.similarity().to_string()
                )
                .increment(1);

                match msg_enum {
                    RbfRibltMessageTypeValues::Handshake => {
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RbfRibltHandshakeMessage>()
                        {
                            this.process_handshake(msg, neighbor).await;
                        } else {
                            error!("Failed to downcast message to RbfRibltHandshakeMessage");
                        }
                    }
                    RbfRibltMessageTypeValues::BloomFilterSlice => {
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RbfRibltBloomFilterSliceMessage>()
                        {
                            this.process_bloom_slice(msg, neighbor).await;
                        } else {
                            error!("Failed to downcast message to RbfRibltBloomFilterSliceMessage");
                        }
                    }
                    RbfRibltMessageTypeValues::RBFStopSignal => {
                        if let Some(_msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RbfRibltRBFStopSignalMessage>()
                        {
                            info!("Received RBF-RIBLT stop signal from {:?}", neighbor);
                            this.protocol
                                .bloom_sending_states
                                .write()
                                .await
                                .remove(&neighbor);
                        } else {
                            error!("Failed to downcast message to RbfRibltRBFStopSignalMessage");
                        }
                    }
                    RbfRibltMessageTypeValues::SComSendSymbol => {
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RbfRibltSComSendSymbolMessage>()
                        {
                            this.handle_scom_send_symbols(msg.clone(), neighbor).await;
                        } else {
                            error!("Failed to downcast message to RbfRibltSComSendSymbolMessage");
                        }
                    }
                    RbfRibltMessageTypeValues::SComDecodedAll => {
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RbfRibltSComDecodedAllMessage>()
                        {
                            this.handle_scom_decoded_all(msg.clone(), neighbor).await;
                        } else {
                            error!("Failed to downcast message to RbfRibltSComDecodedAllMessage");
                        }
                    }
                    RbfRibltMessageTypeValues::SComRequestMoreSymbols => {
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RbfRibltSComRequestMoreSymbolsMessage>()
                        {
                            this.handle_scom_request_more(msg.clone(), neighbor).await;
                        } else {
                            error!(
                                "Failed to downcast message to RbfRibltSComRequestMoreSymbolsMessage"
                            );
                        }
                    }
                    RbfRibltMessageTypeValues::ValueFetchRequest => {
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RbfRibltValueFetchRequestMessage>()
                        {
                            this.handle_value_fetch_request(msg.clone(), neighbor).await;
                        } else {
                            error!(
                                "Failed to downcast message to RbfRibltValueFetchRequestMessage"
                            );
                        }
                    }
                    RbfRibltMessageTypeValues::ValueFetchResponse => {
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RbfRibltValueFetchResponseMessage>()
                        {
                            this.handle_value_fetch_response(msg.clone(), neighbor)
                                .await;
                        } else {
                            error!(
                                "Failed to downcast message to RbfRibltValueFetchResponseMessage"
                            );
                        }
                    }
                }
            }
        });
    }
}
