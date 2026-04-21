use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    hash::{Hash, Hasher},
    sync::Arc,
    time::Instant,
};

use connection::{node::port::NodeAddress, route::RouteTask};
use protocol::deserializer::ProtocolDeserializer;
use riblt::{symbol::PeelableResult, UnmanagedRatelessIBLT};
use runtime::spawn;
use state::node::NodeState;
use tracing::{error, info};

use crate::rbf_riblt::{
    bloom::BloomFilter,
    deserializer::RbfRibltDeserializer,
    messages::{
        RbfRibltBloomFilterSliceMessage, RbfRibltHandshakeMessage,
        RbfRibltMessageTypeValues, RbfRibltSComDecodedAllMessage,
        RbfRibltSComRequestMoreSymbolsMessage, RbfRibltSComSendSymbolMessage,
        RbfRibltValueFetchRequestMessage, RbfRibltValueFetchResponseMessage, RbfRibltFetchedEntry,
    },
    BloomReceivingState, SComReconciliationState, SComReceivingState, BLOOM_C_ELEM,
    RBF_RIBLT_PROTOCOL_ID,
};
use crate::riblt::messages::RIBLTSymbol;

use super::RbfRibltProtocol;

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

        let mut hasher = DefaultHasher::new();
        pairs.len().hash(&mut hasher);
        for (k, v) in pairs {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        Some(hasher.finish())
    }

    async fn process_handshake(&self, msg: &RbfRibltHandshakeMessage, neighbor: NodeAddress) {
        let is_busy = self.protocol.scom_sending_states.read().await.contains_key(&neighbor)
            || self
                .protocol
                .scom_receiving_states
                .read()
                .await
                .contains_key(&neighbor)
            || self
                .protocol
                .pending_value_fetch_sessions
                .read()
                .await
                .contains_key(&neighbor)
            || self
                .protocol
                .bloom_receiving_states
                .read()
                .await
                .get(&neighbor)
                .map(|s| s.riblt_started)
                .unwrap_or(false);

        if is_busy {
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
            self.protocol.bloom_sending_states.write().await.remove(&neighbor);
        }

        info!(
            "Initialized bloom receiving state for {:?} session_id={} m_bits={}",
            neighbor,
            msg.session_id(),
            m_bits
        );
    }

    async fn process_bloom_slice(
        &self,
        msg: &RbfRibltBloomFilterSliceMessage,
        neighbor: NodeAddress,
    ) {
        let storage = match self.protocol.state.get_storage("default".to_string()) {
            Some(s) => s,
            None => return,
        };

        let mut maybe_start_scom = None;

        {
            let mut receiving = self.protocol.bloom_receiving_states.write().await;
            let state = receiving
                .entry(neighbor.clone())
                .or_insert_with(|| BloomReceivingState::new(msg.session_id().to_string(), msg.m()));

            if state.session_id != msg.session_id() {
                *state = BloomReceivingState::new(msg.session_id().to_string(), msg.m());
                self.protocol.bloom_sending_states.write().await.remove(&neighbor);
            }

            let filter = BloomFilter::<String>::from_bytes_with_seeds(
                msg.m(),
                msg.k(),
                msg.seeds(),
                msg.bits().as_slice(),
            );
            state.filters.push(filter);

            state.s_com.clear();
            state.s_tn.clear();

            for item in storage.items() {
                let key = item.key().to_string();
                let positive = state.filters.iter().all(|f| f.contains(&key));
                if positive {
                    state.s_com.push(key);
                } else {
                    state.s_tn.push(key);
                }
            }

            let true_negatives = state.s_tn.len();
            let delta_true_neg = true_negatives.saturating_sub(state.last_true_negatives);
            state.last_true_negatives = true_negatives;

            let threshold = (state.m_bits / BLOOM_C_ELEM).max(1);
            if delta_true_neg < threshold {
                info!(
                    "RBF bloom stop condition reached from {:?}: session={} slice={} delta_true_negatives={} threshold={}",
                    neighbor,
                    msg.session_id(),
                    msg.slice_index(),
                    delta_true_neg,
                    threshold
                );

                if !state.riblt_started {
                    state.riblt_started = true;
                    maybe_start_scom = Some((state.session_id.clone(), state.s_com.clone()));
                }
            }

            info!(
                "RBF bloom partition for {:?}: session={} slice={} s_com={} s_tn={}",
                neighbor,
                msg.session_id(),
                msg.slice_index(),
                state.s_com.len(),
                state.s_tn.len()
            );
        }

        if let Some((session_id, s_com)) = maybe_start_scom {
            info!(
                "Starting S_com rateless reconciliation with {:?} session={} size={}",
                neighbor,
                session_id,
                s_com.len()
            );
            RbfRibltProtocol::start_scom_reconciliation_with_neighbor(
                self.protocol.clone(),
                neighbor.clone(),
                session_id,
                s_com,
            )
            .await;

            self.protocol.bloom_sending_states.write().await.remove(&neighbor);
        }
    }

    fn filter_remote_peeled_symbols(
        peeled_symbols: Vec<PeelableResult<RIBLTSymbol>>,
    ) -> Vec<RIBLTSymbol> {
        peeled_symbols
            .into_iter()
            .filter_map(|symbol| match symbol {
                PeelableResult::Remote(s) => Some(s),
                _ => None,
            })
            .collect()
    }

    async fn init_scom_receiving_state_if_needed(
        &self,
        neighbor: &NodeAddress,
        session_id: &str,
    ) {
        let should_reset = self
            .protocol
            .scom_receiving_states
            .read()
            .await
            .get(neighbor)
            .map(|s| s.session_id != session_id)
            .unwrap_or(false);

        if should_reset {
            self.protocol.scom_receiving_states.write().await.remove(neighbor);
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

        let mut symbols = HashSet::new();
        if let Some(state) = self.protocol.bloom_receiving_states.read().await.get(neighbor) {
            for key in &state.s_com {
                symbols.insert(RIBLTSymbol {
                    key: key.clone(),
                    value: String::new(),
                });
            }
        }

        self.protocol.scom_receiving_states.write().await.insert(
            neighbor.clone(),
            SComReceivingState {
                local_iblt: riblt::RatelessIBLT::new(symbols),
                remote_iblt: UnmanagedRatelessIBLT::new(),
                session_id: session_id.to_string(),
                start_time: Instant::now(),
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

        let (local_coded_symbols, remote_coded_symbols) =
            match self.protocol.scom_receiving_states.write().await.get_mut(&neighbor) {
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

        let (is_peeling_successful, new_hashes) =
            tokio::task::spawn_blocking(move || {
                let local_iblt = UnmanagedRatelessIBLT {
                    coded_symbols: local_coded_symbols,
                };
                let remote_iblt = UnmanagedRatelessIBLT {
                    coded_symbols: remote_coded_symbols,
                };

                let mut collapsed = local_iblt.collapse(&remote_iblt);
                let peel_symbols = collapsed.peel_all_symbols();
                let result = Self::filter_remote_peeled_symbols(peel_symbols);
                let successful = collapsed.is_empty();
                (successful, result)
            })
            .await
            .unwrap();

        if is_peeling_successful {
            let missing_keys: Vec<String> = new_hashes.into_iter().map(|s| s.key).collect();
            self.protocol.scom_receiving_states.write().await.remove(&neighbor);

            self.protocol
                .pending_value_fetch_sessions
                .write()
                .await
                .insert(neighbor.clone(), message.session_id().clone());

            let _ = self
                .protocol
                .state
                .send_through_socket(
                    self.protocol.state.node_identifier().connection_info().clone(),
                    Box::new(neighbor.clone()),
                    Box::new(RbfRibltSComDecodedAllMessage::new(
                        Some(RBF_RIBLT_PROTOCOL_ID),
                        message.session_id().clone(),
                    )),
                )
                .await;

            if !missing_keys.is_empty() {
                let _ = self
                    .protocol
                    .state
                    .send_through_socket(
                        self.protocol.state.node_identifier().connection_info().clone(),
                        Box::new(neighbor),
                        Box::new(RbfRibltValueFetchRequestMessage::new(
                            Some(RBF_RIBLT_PROTOCOL_ID),
                            message.session_id().clone(),
                            missing_keys,
                        )),
                    )
                    .await;
            }
        } else {
            let _ = self
                .protocol
                .state
                .send_through_socket(
                    self.protocol.state.node_identifier().connection_info().clone(),
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
            self.protocol.scom_sending_states.write().await.remove(&neighbor);
            self.protocol
                .pending_value_fetch_sessions
                .write()
                .await
                .insert(neighbor.clone(), message.session_id().clone());

            let request_keys = self
                .protocol
                .bloom_receiving_states
                .read()
                .await
                .get(&neighbor)
                .map(|s| s.s_com.clone())
                .unwrap_or_default();

            if !request_keys.is_empty() {
                let _ = self
                    .protocol
                    .state
                    .send_through_socket(
                        self.protocol.state.node_identifier().connection_info().clone(),
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
    }

    async fn handle_scom_request_more(
        &self,
        message: RbfRibltSComRequestMoreSymbolsMessage,
        neighbor: NodeAddress,
    ) {
        if let Some(status) = self.protocol.scom_sending_states.write().await.get_mut(&neighbor) {
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
        let mut entries = Vec::new();
        for item in storage.items() {
            if wanted.contains(item.key()) {
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
                self.protocol.state.node_identifier().connection_info().clone(),
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

        self.protocol
            .pending_value_fetch_sessions
            .write()
            .await
            .remove(&neighbor);

        self.protocol.scom_sending_states.write().await.remove(&neighbor);
        self.protocol.scom_receiving_states.write().await.remove(&neighbor);
        self.protocol.bloom_sending_states.write().await.remove(&neighbor);

        if let Some(state) = self.protocol.bloom_receiving_states.write().await.get_mut(&neighbor) {
            state.riblt_started = false;
            state.filters.clear();
            state.s_com.clear();
            state.s_tn.clear();
            state.last_true_negatives = 0;
        }

        if let Some(storage) = self.protocol.state.get_storage("default".to_string()) {
            let mut pairs: Vec<(String, String)> = storage
                .items()
                .into_iter()
                .map(|item| (item.key().to_string(), item.value().to_string()))
                .collect();
            pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            use std::hash::{Hash, Hasher};
            pairs.len().hash(&mut hasher);
            for (k, v) in pairs {
                k.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            self.protocol
                .last_reconciled_fingerprint
                .write()
                .await
                .insert(neighbor, hasher.finish());
        }
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
                            error!(
                                "Failed to downcast message to RbfRibltBloomFilterSliceMessage"
                            );
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
                            error!("Failed to downcast message to RbfRibltValueFetchRequestMessage");
                        }
                    }
                    RbfRibltMessageTypeValues::ValueFetchResponse => {
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RbfRibltValueFetchResponseMessage>()
                        {
                            this.handle_value_fetch_response(msg.clone(), neighbor).await;
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
