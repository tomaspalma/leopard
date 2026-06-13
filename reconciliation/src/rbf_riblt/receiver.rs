use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    sync::Arc,
    time::Instant,
};

use connection::{node::port::NodeAddress, route::RouteTask};
use metrics::{counter, gauge};
use protocol::deserializer::ProtocolDeserializer;
use runtime::metrics::experiment::get_context;
use runtime::spawn;
use state::node::NodeState;
use tracing::{error, info};

use crate::riblt_core::{session::store_symbols, RIBLTSymbol};

use crate::algorithms::rbf::bloom::BloomFilter;
use crate::rbf_riblt::{
    deserializer::RbfRibltDeserializer,
    messages::{
        RbfRibltBloomFilterSliceMessage, RbfRibltBloomSliceAckMessage, RbfRibltFetchedEntry,
        RbfRibltHandshakeMessage, RbfRibltMessageTypeValues, RbfRibltRBFStopSignalMessage,
        RbfRibltRequestMoreSymbolsMessage, RbfRibltSComDecodedAllMessage,
        RbfRibltSendSymbolMessage, RbfRibltValueFetchRequestMessage,
        RbfRibltValueFetchResponseMessage,
    },
    BloomReceivingState, BloomSendingState, RBF_RIBLT_PROTOCOL_ID,
};

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
            || self.scom_engine.has_session(neighbor).await
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
        self.scom_engine.has_session(neighbor).await
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
        self.scom_engine.clear(neighbor).await;
        self.round_start_times.write().await.remove(neighbor);
        self.bloom_sending_states.write().await.remove(neighbor);
        self.bloom_receiving_states.write().await.remove(neighbor);
        self.captured_stn.write().await.remove(neighbor);
        self.reconciliation_initiated_with
            .write()
            .await
            .remove(neighbor);
    }

    /// An ack advanced the receiver's processed-slice count: slide the bloom
    /// send window and wake the streaming task.
    pub async fn on_bloom_slice_ack(
        &self,
        neighbor: &NodeAddress,
        session_id: &str,
        processed_count: u64,
    ) {
        if let Some(s) = self.bloom_sending_states.write().await.get_mut(neighbor) {
            if s.session_id == session_id {
                if processed_count > s.acked {
                    s.acked = processed_count;
                }
                s.ack_notify.notify_one();
            }
        }
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

        // Stamp the round clock at first bloom-slice receipt so the responder
        // times the whole reconciliation (bloom + scom), matching the initiator's
        // stamp at bloom-phase start. `or_insert_with` leaves the initiator's
        // earlier stamp untouched when it later receives the reverse bloom stream,
        // and keeps the responder's start from drifting to the scom phase.
        self.protocol
            .round_start_times
            .write()
            .await
            .entry(neighbor.clone())
            .or_insert_with(Instant::now);

        true
    }

    async fn partition_into_state(
        &self,
        neighbor: &NodeAddress,
        filter: &BloomFilter<String>,
    ) -> Option<(bool, String)> {
        let mut receiving = self.protocol.bloom_receiving_states.write().await;
        let state = receiving.get_mut(neighbor)?;

        state.slices_received += 1;

        let prev_s_com = std::mem::take(&mut state.s_com);
        let mut kept = Vec::with_capacity(prev_s_com.len());
        let mut new_true_negatives = 0usize;
        for key in prev_s_com {
            if filter.contains(&key) {
                kept.push(key);
            } else {
                state.s_tn.push(key);
                new_true_negatives += 1;
            }
        }
        state.s_com = kept;

        let stabilized = state.should_stop_slicing(new_true_negatives);
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

    async fn send_slice_ack(
        &self,
        neighbor: NodeAddress,
        session_id: String,
        processed_count: u64,
    ) {
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
                Box::new(RbfRibltBloomSliceAckMessage::new(
                    Some(RBF_RIBLT_PROTOCOL_ID),
                    session_id,
                    processed_count,
                )),
            )
            .await;
    }

    /// Records the post-bloom candidate-set size (`|s_com|`) and the number of
    /// bloom slices applied (`S`) for this peer, tagged like every other
    /// per-trial metric. Emitted once per session when `s_com` is finalized, so
    /// the similarity analysis can relate both quantities to the similarity level.
    fn record_scom_metrics(&self, neighbor: &NodeAddress, scom_size: usize, slices: usize) {
        let context = get_context();
        gauge!(
            "scom_size",
            "protocol" => "rbf_riblt",
            "target" => format!("{:?}", neighbor),
            "run_id" => context.run_id().to_string(),
            "trial" => context.trial().to_string(),
            "similarity" => context.similarity().to_string()
        )
        .set(scom_size as f64);

        gauge!(
            "bloom_slices",
            "protocol" => "rbf_riblt",
            "target" => format!("{:?}", neighbor),
            "run_id" => context.run_id().to_string(),
            "trial" => context.trial().to_string(),
            "similarity" => context.similarity().to_string()
        )
        .set(slices as f64);
    }

    async fn start_scom_phase(&self, neighbor: NodeAddress) {
        info!("Starting scom phase");
        // Atomically check+set riblt_started to prevent duplicate starts
        // from in-flight bloom slices arriving after stabilization.
        let (s_com, s_tn, slices) = {
            let mut receiving = self.protocol.bloom_receiving_states.write().await;
            match receiving.get_mut(&neighbor) {
                Some(state) if !state.riblt_started => {
                    state.riblt_started = true;
                    (state.s_com.clone(), state.s_tn.clone(), state.slices_received)
                }
                _ => return,
            }
        };

        self.record_scom_metrics(&neighbor, s_com.len(), slices);

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

        // Stream the s_com subset through the shared engine. The decoder on the
        // other side is seeded from its own s_com via the sink's seed_symbols.
        self.protocol
            .scom_engine
            .start_send(neighbor, symbols, session_id)
            .await;
    }

    async fn start_reverse_stream(&self, neighbor: NodeAddress) {
        info!("Starting reverse stream phase");
        let (s_com, s_tn, slices) = {
            let receiving = self.protocol.bloom_receiving_states.read().await;
            match receiving.get(&neighbor) {
                Some(s) => (s.s_com.clone(), s.s_tn.clone(), s.slices_received),
                None => return,
            }
        };

        self.record_scom_metrics(&neighbor, s_com.len(), slices);

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

        let (stabilized, session_id) = match self.partition_into_state(&neighbor, &filter).await {
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
        } else {
            // Not yet stable: ack this slice so the sender releases the next one.
            self.send_slice_ack(neighbor, session_id, msg.slice_index() + 1)
                .await;
        }
    }

    async fn handle_scom_decoded_all(
        &self,
        message: RbfRibltSComDecodedAllMessage,
        neighbor: NodeAddress,
    ) {
        // Stop our sender for this session; only proceed if we were actually
        // streaming to this neighbor under this session.
        let was_sending = self
            .protocol
            .scom_engine
            .on_finished(&neighbor, message.session_id())
            .await;

        if was_sending {
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

        let symbols: Vec<RIBLTSymbol> = message
            .entries()
            .iter()
            .map(|e| RIBLTSymbol {
                key: e.key().to_string(),
                value: e.value().to_string(),
            })
            .collect();
        store_symbols(&self.protocol.state, symbols).await;

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
                    RbfRibltMessageTypeValues::BloomSliceAck => {
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RbfRibltBloomSliceAckMessage>()
                        {
                            this.protocol
                                .on_bloom_slice_ack(
                                    &neighbor,
                                    msg.session_id(),
                                    msg.processed_count(),
                                )
                                .await;
                        } else {
                            error!("Failed to downcast message to RbfRibltBloomSliceAckMessage");
                        }
                    }
                    RbfRibltMessageTypeValues::RBFStopSignal => {
                        if let Some(_msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RbfRibltRBFStopSignalMessage>()
                        {
                            info!("Received RBF-RIBLT stop signal from {:?}", neighbor);
                            // Wake a parked streaming task so it observes the
                            // removal and stops instead of waiting out the ack
                            // timeout.
                            if let Some(s) = this
                                .protocol
                                .bloom_sending_states
                                .write()
                                .await
                                .remove(&neighbor)
                            {
                                s.ack_notify.notify_one();
                            }
                        } else {
                            error!("Failed to downcast message to RbfRibltRBFStopSignalMessage");
                        }
                    }
                    RbfRibltMessageTypeValues::SendSymbol => {
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RbfRibltSendSymbolMessage>()
                        {
                            this.protocol
                                .scom_engine
                                .on_symbols(
                                    neighbor,
                                    msg.session_id().clone(),
                                    msg.start_index(),
                                    msg.symbols().clone(),
                                )
                                .await;
                        } else {
                            error!("Failed to downcast message to RbfRibltSendSymbolMessage");
                        }
                    }
                    RbfRibltMessageTypeValues::RequestMoreSymbols => {
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RbfRibltRequestMoreSymbolsMessage>()
                        {
                            this.protocol
                                .scom_engine
                                .on_request_more(
                                    &neighbor,
                                    msg.session_id(),
                                    msg.received_count(),
                                )
                                .await;
                        } else {
                            error!(
                                "Failed to downcast message to RbfRibltRequestMoreSymbolsMessage"
                            );
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
