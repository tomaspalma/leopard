use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    sync::Arc,
    time::Instant,
};

use connection::{node::port::NodeAddress, route::RouteTask};
use metrics::{counter, gauge, histogram};
use protocol::deserializer::ProtocolDeserializer;
use crate::algorithms::ribbon::{Mode, Params, RibbonFilter};
use riblt::{Decoder, RatelessIBLT};
use runtime::metrics::experiment::get_context;
use runtime::spawn;
use state::node::NodeState;
use tracing::{error, info};

use crate::riblt_core::{
    session::{add_coded_symbols, store_symbols, try_decode_blocking},
    RIBLTSymbol,
};

use crate::rf_riblt::{
    deserializer::RfRibltDeserializer,
    messages::{
        RfRibltFetchedEntry, RfRibltHandshakeMessage, RfRibltMessageTypeValues,
        RfRibltSComDecodedAllMessage, RfRibltSComRequestMoreSymbolsMessage,
        RfRibltSComSendSymbolMessage, RfRibltValueFetchRequestMessage,
        RfRibltValueFetchResponseMessage,
    },
    RfFilterReceivingState, RfRibltHasher, SComReceivingState, SComReconciliationState,
    SComSendingState, RF_RIBLT_PROTOCOL_ID,
};

use super::RfRibltProtocol;

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

impl RfRibltProtocol {
    pub async fn is_session_active(&self, neighbor: &NodeAddress) -> bool {
        self.filter_receiving_states
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
                .filter_receiving_states
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
        self.filter_receiving_states.write().await.remove(neighbor);
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

pub struct ReceiveRfRibltMessageTask {
    protocol: Arc<RfRibltProtocol>,
}

impl ReceiveRfRibltMessageTask {
    pub fn new(protocol: Arc<RfRibltProtocol>) -> Self {
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


    async fn process_handshake(&self, msg: &RfRibltHandshakeMessage, neighbor: NodeAddress) {
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

        // Determine if we should send our own filter back before mutating state.
        // We send back if: we haven't already received from this neighbor AND we didn't initiate.
        let already_received = self
            .protocol
            .filter_receiving_states
            .read()
            .await
            .get(&neighbor)
            .map(|s| s.session_id == msg.session_id)
            .unwrap_or(false);
        let already_initiated = self
            .protocol
            .reconciliation_initiated_with
            .read()
            .await
            .contains(&neighbor);
        let should_send_reverse = !already_received && !already_initiated;

        // Create or reset the receiving state for this session.
        {
            let mut receiving = self.protocol.filter_receiving_states.write().await;
            let should_reset = receiving
                .get(&neighbor)
                .map(|st| st.session_id != msg.session_id)
                .unwrap_or(true);
            if should_reset {
                receiving.insert(
                    neighbor.clone(),
                    RfFilterReceivingState::new(msg.session_id.clone()),
                );
            }
        }

        // Reconstruct the filter from the bytes embedded in this message.
        let filter = match Self::reconstruct_filter_from_bytes(&msg.filter_bytes) {
            Some(f) => f,
            None => {
                error!("Failed to reconstruct ribbon filter from {:?}", neighbor);
                return;
            }
        };

        // Partition local keys against the neighbor's filter.
        let local_keys: Vec<String> = self
            .protocol
            .state
            .get_storage("default".to_string())
            .map(|s| s.items().into_iter().map(|i| i.key().to_string()).collect())
            .unwrap_or_default();

        let mut s_com = Vec::new();
        let mut s_tn = Vec::new();
        for key in local_keys {
            if filter.contains(&key) {
                s_com.push(key);
            } else {
                s_tn.push(key);
            }
        }

        let s_com_len = s_com.len();
        let s_tn_len = s_tn.len();

        {
            let mut receiving = self.protocol.filter_receiving_states.write().await;
            if let Some(state) = receiving.get_mut(&neighbor) {
                if state.session_id == msg.session_id {
                    state.s_com = s_com;
                    state.s_tn = s_tn;
                }
            }
        }

        info!(
            "Processed ribbon filter from {:?}: {} s_com, {} s_tn (reverse={})",
            neighbor, s_com_len, s_tn_len, should_send_reverse
        );

        // Send our own filter back if this is the responder's first time seeing the initiator.
        if should_send_reverse {
            let storage = match self.protocol.state.get_storage("default".to_string()) {
                Some(s) => s,
                None => return,
            };
            let keys: Vec<String> = storage
                .items()
                .into_iter()
                .map(|item| item.key().to_string())
                .collect();

            let (filter_bytes, ribbon_seed) =
                match RfRibltProtocol::build_filter_bytes(&keys) {
                    Some(result) => result,
                    None => {
                        error!("Failed to build ribbon filter for reverse send to {:?}", neighbor);
                        return;
                    }
                };

            let session_id = uuid::Uuid::new_v4().to_string();
            let state = self.protocol.state.clone();

            let neighbor_for_send = neighbor.clone();
            spawn!({
                RfRibltProtocol::send_filter_to_neighbor(
                    state,
                    neighbor_for_send,
                    session_id,
                    filter_bytes,
                    ribbon_seed,
                )
                .await;
            });
        }

        // Initiator starts the RIBLT phase once it has processed the responder's filter.
        if already_initiated {
            self.start_scom_phase(neighbor).await;
        }
    }

    /// Deserialize a ribbon filter from the bit-packed format written by `build_filter_bytes`.
    /// Format: [m:u64][w:u64][r:u64][seed:u64][mode:u8][packed_len:u64][packed: ceil(r/8) bytes/row]
    fn reconstruct_filter_from_bytes(bytes: &[u8]) -> Option<RibbonFilter<RfRibltHasher>> {
        if bytes.len() < 8 * 4 + 1 + 8 {
            return None;
        }
        let mut off = 0usize;

        macro_rules! read_u64 {
            () => {{
                if off + 8 > bytes.len() {
                    return None;
                }
                let arr: [u8; 8] = bytes[off..off + 8].try_into().ok()?;
                off += 8;
                u64::from_le_bytes(arr)
            }};
        }

        let m = read_u64!() as usize;
        let w = read_u64!() as usize;
        let r = read_u64!() as usize;
        let seed = read_u64!();
        if off >= bytes.len() {
            return None;
        }
        let mode = match bytes[off] {
            0 => Mode::Standard,
            _ => Mode::Homogeneous,
        };
        off += 1;
        let packed_len = read_u64!() as usize;
        if bytes.len() < off + packed_len {
            return None;
        }

        let stride_words = r.div_ceil(64);
        let mut z_raw = vec![0u64; m * stride_words];
        for row in 0..m {
            let base = row * stride_words;
            let mut bits_left = r;
            let mut word_i = 0;
            while bits_left > 0 {
                let take = bits_left.min(64);
                let take_bytes = take.div_ceil(8);
                if off + take_bytes > bytes.len() {
                    return None;
                }
                let mut word_bytes = [0u8; 8];
                word_bytes[..take_bytes].copy_from_slice(&bytes[off..off + take_bytes]);
                z_raw[base + word_i] = u64::from_le_bytes(word_bytes);
                off += take_bytes;
                bits_left -= take;
                word_i += 1;
            }
        }

        let params = Params::new(m, w, r, mode).ok()?.with_seed(seed);
        Some(RibbonFilter::from_raw_parts(
            params,
            RfRibltHasher::default(),
            z_raw,
        ))
    }

    async fn start_scom_phase(&self, neighbor: NodeAddress) {
        info!("Starting RF-RIBLT scom phase for {:?}", neighbor);

        let (s_com, s_tn) = {
            let mut receiving = self.protocol.filter_receiving_states.write().await;
            match receiving.get_mut(&neighbor) {
                Some(state) if !state.riblt_started => {
                    state.riblt_started = true;
                    (state.s_com.clone(), state.s_tn.clone())
                }
                _ => return,
            }
        };

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
            RfRibltProtocol::stream_scom_symbols_to_neighbor(state, scom_sending_states, neighbor)
                .await;
        });
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
            .filter_receiving_states
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

        let mut decoder = Decoder::new();
        for symbol in symbols {
            decoder.add_symbol(symbol);
        }
        self.protocol.scom_receiving_states.write().await.insert(
            neighbor.clone(),
            SComReceivingState {
                decoder,
                session_id: session_id.to_string(),
            },
        );
    }

    async fn handle_scom_send_symbols(
        &self,
        message: RfRibltSComSendSymbolMessage,
        neighbor: NodeAddress,
    ) {
        self.init_scom_receiving_state_if_needed(&neighbor, &message.session_id)
            .await;

        let decoder = match self
            .protocol
            .scom_receiving_states
            .write()
            .await
            .get_mut(&neighbor)
        {
            Some(status) => {
                add_coded_symbols(&mut status.decoder, &message.symbols);
                std::mem::replace(&mut status.decoder, Decoder::new())
            }
            None => return,
        };

        let decode_start = std::time::Instant::now();
        let (decoder, peel_result) = try_decode_blocking(decoder, 0).await;

        if let Some(status) = self
            .protocol
            .scom_receiving_states
            .write()
            .await
            .get_mut(&neighbor)
        {
            if status.session_id == message.session_id {
                status.decoder = decoder;
            }
        }
        histogram!(
            "rf_riblt_decode_duration_seconds",
            "neighbor" => format!("{:?}", neighbor)
        )
        .record(decode_start.elapsed().as_secs_f64());

        let is_peeling_successful = peel_result.successful;
        let remote_symbols = peel_result.remote_symbols;
        let local_symbols = peel_result.local_symbols;

        if is_peeling_successful {
            let mut keys_for_sender: Vec<String> =
                local_symbols.into_iter().map(|s| s.key).collect();
            let s_tn: Vec<String> = self
                .protocol
                .filter_receiving_states
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
                .insert(neighbor.clone(), message.session_id.clone());

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
                    Box::new(crate::rf_riblt::messages::RfRibltSComDecodedAllMessage::new(
                        Some(RF_RIBLT_PROTOCOL_ID),
                        message.session_id.clone(),
                        keys_for_sender,
                    )),
                )
                .await;

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
                    Box::new(RfRibltValueFetchRequestMessage::new(
                        Some(RF_RIBLT_PROTOCOL_ID),
                        message.session_id,
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
                    Box::new(RfRibltSComRequestMoreSymbolsMessage::new(
                        Some(RF_RIBLT_PROTOCOL_ID),
                        message.session_id,
                    )),
                )
                .await;
        }
    }

    async fn handle_scom_decoded_all(
        &self,
        message: RfRibltSComDecodedAllMessage,
        neighbor: NodeAddress,
    ) {
        let should_remove = self
            .protocol
            .scom_sending_states
            .read()
            .await
            .get(&neighbor)
            .map(|state| state.session_id == message.session_id)
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
                .insert(neighbor.clone(), message.session_id.clone());

            let request_keys = message.keys_for_sender.clone();
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
                    Box::new(RfRibltValueFetchRequestMessage::new(
                        Some(RF_RIBLT_PROTOCOL_ID),
                        message.session_id,
                        request_keys,
                    )),
                )
                .await;
        }
    }

    async fn handle_scom_request_more(
        &self,
        message: RfRibltSComRequestMoreSymbolsMessage,
        neighbor: NodeAddress,
    ) {
        if let Some(status) = self
            .protocol
            .scom_sending_states
            .write()
            .await
            .get_mut(&neighbor)
        {
            if status.session_id == message.session_id {
                status.state = SComReconciliationState::SendingSymbols;
            }
        }
    }

    async fn handle_value_fetch_request(
        &self,
        message: RfRibltValueFetchRequestMessage,
        neighbor: NodeAddress,
    ) {
        let storage = match self.protocol.state.get_storage("default".to_string()) {
            Some(s) => s,
            None => return,
        };

        let wanted: HashSet<&str> = message.keys.iter().map(String::as_str).collect();

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
                entries.push(RfRibltFetchedEntry::new(
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
                Box::new(RfRibltValueFetchResponseMessage::new(
                    Some(RF_RIBLT_PROTOCOL_ID),
                    message.session_id,
                    entries,
                )),
            )
            .await;
    }

    async fn handle_value_fetch_response(
        &self,
        message: RfRibltValueFetchResponseMessage,
        neighbor: NodeAddress,
    ) {
        let pending = self
            .protocol
            .pending_value_fetch_sessions
            .read()
            .await
            .get(&neighbor)
            .cloned();

        if pending.as_deref() != Some(message.session_id.as_str()) {
            return;
        }

        let symbols: Vec<RIBLTSymbol> = message
            .entries
            .iter()
            .map(|e| RIBLTSymbol {
                key: e.key.clone(),
                value: e.value.clone(),
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
                "protocol" => "rf_riblt",
                "neighbor" => format!("{:?}", neighbor),
                "run_id" => context.run_id().to_string(),
                "trial" => context.trial().to_string(),
                "similarity" => context.similarity().to_string()
            )
            .set(duration);
        }

        counter!(
            "reconciliation_completed",
            "protocol" => "rf_riblt",
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
            "rf_riblt",
        );
    }
}

impl RouteTask for ReceiveRfRibltMessageTask {
    fn run(self: Arc<Self>, message: Vec<u8>, neighbor: NodeAddress) {
        let deserialized_message = RfRibltDeserializer::new().deserialize(message);
        let msg_type_box = deserialized_message.get_type().value();

        let msg_type = msg_type_box
            .as_any()
            .downcast_ref::<RfRibltMessageTypeValues>()
            .cloned();

        let this = self.clone();
        spawn!({
            if let Some(msg_enum) = msg_type {
                let context = get_context();
                counter!(
                    "protocol_round_trip_count",
                    "target" => format!("{:?}", neighbor),
                    "protocol" => "rf_riblt",
                    "run_id" => context.run_id().to_string(),
                    "trial" => context.trial().to_string(),
                    "similarity" => context.similarity().to_string()
                )
                .increment(1);

                match msg_enum {
                    RfRibltMessageTypeValues::Handshake => {
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RfRibltHandshakeMessage>()
                        {
                            this.process_handshake(msg, neighbor).await;
                        } else {
                            error!("Failed to downcast to RfRibltHandshakeMessage");
                        }
                    }
                    RfRibltMessageTypeValues::FilterChunk
                    | RfRibltMessageTypeValues::FilterDone => {
                        // Filter is now delivered entirely within the Handshake message.
                    }
                    RfRibltMessageTypeValues::SComSendSymbol => {
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RfRibltSComSendSymbolMessage>()
                        {
                            this.handle_scom_send_symbols(msg.clone(), neighbor).await;
                        } else {
                            error!("Failed to downcast to RfRibltSComSendSymbolMessage");
                        }
                    }
                    RfRibltMessageTypeValues::SComDecodedAll => {
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RfRibltSComDecodedAllMessage>()
                        {
                            this.handle_scom_decoded_all(msg.clone(), neighbor).await;
                        } else {
                            error!("Failed to downcast to RfRibltSComDecodedAllMessage");
                        }
                    }
                    RfRibltMessageTypeValues::SComRequestMoreSymbols => {
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RfRibltSComRequestMoreSymbolsMessage>()
                        {
                            this.handle_scom_request_more(msg.clone(), neighbor).await;
                        } else {
                            error!("Failed to downcast to RfRibltSComRequestMoreSymbolsMessage");
                        }
                    }
                    RfRibltMessageTypeValues::ValueFetchRequest => {
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RfRibltValueFetchRequestMessage>()
                        {
                            this.handle_value_fetch_request(msg.clone(), neighbor).await;
                        } else {
                            error!("Failed to downcast to RfRibltValueFetchRequestMessage");
                        }
                    }
                    RfRibltMessageTypeValues::ValueFetchResponse => {
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RfRibltValueFetchResponseMessage>()
                        {
                            this.handle_value_fetch_response(msg.clone(), neighbor).await;
                        } else {
                            error!("Failed to downcast to RfRibltValueFetchResponseMessage");
                        }
                    }
                }
            }
        });
    }
}
