pub mod deserializer;
pub mod messages;
pub mod protocols;
pub mod receiver;
pub mod session;
pub mod stream;

pub use deserializer::RIBLTDeserializer;

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use metrics::{counter, gauge};
use runtime::metrics::experiment::{get_context, ExperimentContext};
use tracing::info;

use state::node::{DefaultNodeState, NodeState};

use connection::node::port::NodeAddress;
use membership::Membership;

use crate::riblt::messages::{
    RIBLTDecodedAllMessage, RIBLTMessageType, RIBLTMessageTypeValues, RIBLTRequestMoreSymbolsMessage,
    RIBLTSendSymbolMessage, RIBLTSymbol,
};
use crate::riblt::messages::RIBLTCodedSymbol;
use crate::riblt::stream::{RibltDecodeSink, RibltStreamEngine, RibltStreamTransport};

/// Reconciliation phase for the bloom/filter-based protocols that reuse this
/// enum (rf_riblt, rbf_riblt). The plain RIBLT protocol no longer uses it: it
/// streams symbols continuously under a credit window (see `stream`).
#[derive(Debug, Clone, PartialEq)]
pub enum ReconciliationState {
    SendingSymbols,
    AwaitingConfirmation,
}

pub const RIBLT_PROTOCOL_ID: u64 = protocol::ProtocolId::Riblt as u64;

/// Wire adapter: turns engine send calls into RIBLT protocol messages.
struct RibltTransport {
    state: Arc<DefaultNodeState>,
    own_id: NodeAddress,
    protocol_id: u64,
}

#[async_trait]
impl RibltStreamTransport for RibltTransport {
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
                Box::new(RIBLTSendSymbolMessage::new(
                    RIBLTMessageType::new(RIBLTMessageTypeValues::SendSymbol),
                    Some(self.protocol_id),
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
                Box::new(RIBLTRequestMoreSymbolsMessage::new(
                    RIBLTMessageType::new(RIBLTMessageTypeValues::RequestMoreSymbols),
                    Some(self.protocol_id),
                    session_id.to_string(),
                    received_count,
                )),
            )
            .await;
    }

    async fn send_finished(&self, neighbor: &NodeAddress, session_id: &str) {
        let _ = self
            .state
            .send_through_socket(
                self.own_id.clone(),
                Box::new(neighbor.clone()),
                Box::new(RIBLTDecodedAllMessage::new(
                    RIBLTMessageType::new(RIBLTMessageTypeValues::FinishedDecoding),
                    Some(self.protocol_id),
                    session_id.to_string(),
                )),
            )
            .await;
    }
}

/// Emit the seed/decode/difference split for one completed IBLT session,
/// tagged by protocol. Shared by riblt and rbf_riblt's scom phase (both run
/// through the same streaming engine) so the two are directly comparable.
pub(crate) fn record_phase_split(
    protocol: &'static str,
    neighbor: &NodeAddress,
    context: &ExperimentContext,
    seed_secs: f64,
    decode_secs: f64,
    decoded_difference: usize,
    round_trips: u64,
) {
    gauge!(
        "reconciliation_round_trips",
        "protocol" => protocol,
        "neighbor" => format!("{:?}", neighbor),
        "run_id" => context.run_id().to_string(),
        "trial" => context.trial().to_string(),
        "similarity" => context.similarity().to_string()
    )
    .set(round_trips as f64);
    gauge!(
        "reconciliation_seed_seconds",
        "protocol" => protocol,
        "neighbor" => format!("{:?}", neighbor),
        "run_id" => context.run_id().to_string(),
        "trial" => context.trial().to_string(),
        "similarity" => context.similarity().to_string()
    )
    .set(seed_secs);
    gauge!(
        "reconciliation_decode_seconds",
        "protocol" => protocol,
        "neighbor" => format!("{:?}", neighbor),
        "run_id" => context.run_id().to_string(),
        "trial" => context.trial().to_string(),
        "similarity" => context.similarity().to_string()
    )
    .set(decode_secs);
    gauge!(
        "reconciliation_decoded_difference",
        "protocol" => protocol,
        "neighbor" => format!("{:?}", neighbor),
        "run_id" => context.run_id().to_string(),
        "trial" => context.trial().to_string(),
        "similarity" => context.similarity().to_string()
    )
    .set(decoded_difference as f64);
}

/// Decode adapter: seeds from the full local store, persists decoded remote
/// symbols, and records completion metrics.
struct RibltSink {
    state: Arc<DefaultNodeState>,
    own_id: NodeAddress,
}

#[async_trait]
impl RibltDecodeSink for RibltSink {
    async fn seed_symbols(&self, _neighbor: &NodeAddress) -> HashSet<RIBLTSymbol> {
        session::load_iblt_symbols(&self.state)
    }

    async fn on_remote_symbols(
        &self,
        _neighbor: &NodeAddress,
        _session_id: &str,
        new_remote: Vec<RIBLTSymbol>,
    ) {
        session::store_symbols(&self.state, new_remote).await;
    }

    async fn on_complete(
        &self,
        neighbor: &NodeAddress,
        _session_id: &str,
        _local_only: Vec<RIBLTSymbol>,
        round_secs: f64,
        seed_secs: f64,
        decode_secs: f64,
        decoded_difference: usize,
        round_trips: u64,
    ) {
        info!("Peeling successful for neighbor {:?}", neighbor);
        let context = get_context();
        gauge!(
            "reconciliation_round_duration_seconds",
            "protocol" => "riblt",
            "neighbor" => format!("{:?}", neighbor),
            "run_id" => context.run_id().to_string(),
            "trial" => context.trial().to_string(),
            "similarity" => context.similarity().to_string()
        )
        .set(round_secs);
        record_phase_split(
            "riblt",
            neighbor,
            &context,
            seed_secs,
            decode_secs,
            decoded_difference,
            round_trips,
        );
        counter!(
            "reconciliation_completed",
            "protocol" => "riblt",
            "neighbor" => format!("{:?}", neighbor),
            "run_id" => context.run_id().to_string(),
            "trial" => context.trial().to_string(),
            "similarity" => context.similarity().to_string()
        )
        .increment(1);
        runtime::metrics::csv::finish_iteration(
            format!("{:?}", self.own_id),
            format!("{:?}", neighbor),
            "riblt",
        );
    }
}

pub struct RIBLT {
    id: u64,
    state: Arc<DefaultNodeState>,
    pub(crate) port: NodeAddress,
    deserializer: Arc<RIBLTDeserializer>,
    pub engine: Arc<RibltStreamEngine>,
}

impl RIBLT {
    pub fn new(state: Arc<DefaultNodeState>, port: NodeAddress) -> Self {
        let own_id = state.node_identifier().connection_info().clone();
        let transport = Arc::new(RibltTransport {
            state: state.clone(),
            own_id: own_id.clone(),
            protocol_id: RIBLT_PROTOCOL_ID,
        });
        let sink = Arc::new(RibltSink {
            state: state.clone(),
            own_id,
        });
        let engine = Arc::new(RibltStreamEngine::new(transport, sink));
        Self {
            id: RIBLT_PROTOCOL_ID,
            state,
            port,
            deserializer: Arc::new(RIBLTDeserializer::default()),
            engine,
        }
    }

    async fn reconciliation_mechanism(
        state: Arc<DefaultNodeState>,
        engine: Arc<RibltStreamEngine>,
    ) -> Result<(), String> {
        info!("Ran reconciliation mechanism");

        let connection_targets = state.membership().read().await.valid_connection_targets();
        info!("Valid connection targets: {:?}", connection_targets);

        for info in connection_targets {
            if engine.already_sending(&info).await {
                info!("Already reconciling with neighbor {:?}, skipping", info);
                continue;
            }
            let symbols = session::load_iblt_symbols(&state);
            engine
                .start_send(info, symbols, uuid::Uuid::new_v4().to_string())
                .await;
        }

        Ok(())
    }
}
