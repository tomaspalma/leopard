//! rbf_riblt's host adapters for the shared riblt streaming engine.
//!
//! The scom phase reconciles the post-bloom `s_com` subsets with a rateless-IBLT
//! exchange. These adapters plug that exchange into `riblt::stream`: the
//! transport builds rbf's scom wire messages, and the sink seeds the decoder
//! from `s_com`, accumulates the decoded difference, and drives rbf's
//! value-fetch completion (which also carries `s_tn` keys the IBLT never sees).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use connection::node::port::NodeAddress;
use state::node::{DefaultNodeState, NodeState};
use tokio::sync::RwLock;

use runtime::metrics::experiment::get_context;

use crate::riblt::messages::{RIBLTCodedSymbol, RIBLTSymbol};
use crate::riblt::record_phase_split;
use crate::riblt::stream::{RibltDecodeSink, RibltStreamTransport};

use crate::rbf_riblt::messages::{
    RbfRibltSComDecodedAllMessage, RbfRibltSComRequestMoreSymbolsMessage,
    RbfRibltSComSendSymbolMessage, RbfRibltValueFetchRequestMessage,
};
use crate::rbf_riblt::{BloomReceivingState, RBF_RIBLT_PROTOCOL_ID};

/// Wire adapter: turns engine send calls into rbf scom messages.
pub struct RbfScomTransport {
    pub state: Arc<DefaultNodeState>,
    pub own_id: NodeAddress,
}

#[async_trait]
impl RibltStreamTransport for RbfScomTransport {
    async fn send_symbols(
        &self,
        neighbor: &NodeAddress,
        session_id: &str,
        start_index: u64,
        symbols: Vec<RIBLTCodedSymbol>,
    ) {
        let _ = self
            .state
            .send_through_socket(
                self.own_id.clone(),
                Box::new(neighbor.clone()),
                Box::new(RbfRibltSComSendSymbolMessage::new(
                    Some(RBF_RIBLT_PROTOCOL_ID),
                    symbols,
                    session_id.to_string(),
                    start_index,
                )),
            )
            .await;
    }

    async fn send_request_more(
        &self,
        neighbor: &NodeAddress,
        session_id: &str,
        received_count: u64,
    ) {
        let _ = self
            .state
            .send_through_socket(
                self.own_id.clone(),
                Box::new(neighbor.clone()),
                Box::new(RbfRibltSComRequestMoreSymbolsMessage::new(
                    Some(RBF_RIBLT_PROTOCOL_ID),
                    session_id.to_string(),
                    received_count,
                )),
            )
            .await;
    }

    async fn send_finished(&self, _neighbor: &NodeAddress, _session_id: &str) {
        // No-op: rbf signals completion with SComDecodedAll, which carries the
        // keys_for_sender payload and is sent from the sink's on_complete.
    }
}

/// Decode adapter: seeds the decoder from the local `s_com` subset, accumulates
/// the decoded remote difference, and on completion kicks off rbf's value-fetch.
pub struct RbfScomSink {
    pub state: Arc<DefaultNodeState>,
    pub own_id: NodeAddress,
    pub bloom_receiving_states: Arc<RwLock<HashMap<NodeAddress, BloomReceivingState>>>,
    pub pending_value_fetch_sessions: Arc<RwLock<HashMap<NodeAddress, String>>>,
    pub round_start_times: Arc<RwLock<HashMap<NodeAddress, Instant>>>,
    // Remote-only keys decoded so far this session, accumulated across batches
    // until completion (rbf fetches their values rather than storing directly).
    pub pending_remote: Arc<RwLock<HashMap<NodeAddress, Vec<String>>>>,
}

#[async_trait]
impl RibltDecodeSink for RbfScomSink {
    async fn seed_symbols(&self, neighbor: &NodeAddress) -> HashSet<RIBLTSymbol> {
        let s_com: Vec<String> = self
            .bloom_receiving_states
            .read()
            .await
            .get(neighbor)
            .map(|s| s.s_com.clone())
            .unwrap_or_default();

        let storage = self.state.get_storage("default".to_string());
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

        // Fallback start stamp for the round clock (read later by
        // handle_value_fetch_response): the initiator already stamps the
        // reconciliation start when it begins the bloom phase, so only stamp
        // here if no earlier start exists. Also reset the per-session remote
        // accumulator.
        self.round_start_times
            .write()
            .await
            .entry(neighbor.clone())
            .or_insert_with(Instant::now);
        self.pending_remote
            .write()
            .await
            .insert(neighbor.clone(), Vec::new());

        symbols
    }

    async fn on_remote_symbols(
        &self,
        neighbor: &NodeAddress,
        _session_id: &str,
        new_remote: Vec<RIBLTSymbol>,
    ) {
        let mut acc = self.pending_remote.write().await;
        acc.entry(neighbor.clone())
            .or_default()
            .extend(new_remote.into_iter().map(|s| s.key));
    }

    async fn on_complete(
        &self,
        neighbor: &NodeAddress,
        session_id: &str,
        local_only: Vec<RIBLTSymbol>,
        _round_secs: f64,
        seed_secs: f64,
        decode_secs: f64,
        decoded_difference: usize,
    ) {
        // Attribute the scom phase between seeding the decoder from s_com
        // (O(|s_com|)) and peeling the false-positive difference (O(difference)).
        record_phase_split(
            "rbf_riblt",
            neighbor,
            &get_context(),
            seed_secs,
            decode_secs,
            decoded_difference,
        );

        // Keys the neighbor is missing: our local-only IBLT elements plus our s_tn
        // (keys definitely absent from their bloom filter, never seen by IBLT).
        let mut keys_for_sender: Vec<String> =
            local_only.into_iter().map(|s| s.key).collect();
        let s_tn: Vec<String> = self
            .bloom_receiving_states
            .read()
            .await
            .get(neighbor)
            .map(|s| s.s_tn.clone())
            .unwrap_or_default();
        keys_for_sender.extend(s_tn);

        // Keys we are missing: the decoded remote-only difference.
        let missing_keys = self
            .pending_remote
            .write()
            .await
            .remove(neighbor)
            .unwrap_or_default();

        self.pending_value_fetch_sessions
            .write()
            .await
            .insert(neighbor.clone(), session_id.to_string());

        let _ = self
            .state
            .send_through_socket(
                self.own_id.clone(),
                Box::new(neighbor.clone()),
                Box::new(RbfRibltSComDecodedAllMessage::new(
                    Some(RBF_RIBLT_PROTOCOL_ID),
                    session_id.to_string(),
                    keys_for_sender,
                )),
            )
            .await;

        // Always send a fetch request so the responder piggybacks their s_tn
        // values, even when missing_keys is empty.
        let _ = self
            .state
            .send_through_socket(
                self.own_id.clone(),
                Box::new(neighbor.clone()),
                Box::new(RbfRibltValueFetchRequestMessage::new(
                    Some(RBF_RIBLT_PROTOCOL_ID),
                    session_id.to_string(),
                    missing_keys,
                )),
            )
            .await;
    }
}
