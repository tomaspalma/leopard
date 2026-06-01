pub mod deserializer;
pub mod messages;
pub mod protocols;
pub mod receiver;
pub mod session;

pub use deserializer::RIBLTDeserializer;

use runtime::spawn;

use std::collections::HashMap;
use tokio::sync::RwLock;

use std::sync::Arc;
use tracing::info;

use state::{
    node::{DefaultNodeState, NodeState},
};

use connection::node::port::NodeAddress;
use membership::Membership;

use crate::riblt::messages::{
    RIBLTCodedSymbol, RIBLTMessageType, RIBLTMessageTypeValues, RIBLTSendSymbolMessage, RIBLTSymbol,
};
use riblt::RatelessIBLT;

use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio::time::{timeout, Duration};

/// Reconciliation phase for the bloom/filter-based protocols that reuse this
/// enum (rf_riblt, rbf_riblt). The plain RIBLT protocol no longer uses it: it
/// streams symbols continuously under a credit window rather than stopping to
/// wait for a per-batch confirmation.
#[derive(Debug, Clone, PartialEq)]
pub enum ReconciliationState {
    SendingSymbols,
    AwaitingConfirmation,
}

use std::time::Instant;

pub struct SendingState {
    pub local_iblt: RatelessIBLT<RIBLTSymbol>,
    pub start_time: Instant,
    pub session_id: String,
    // Woken whenever an acknowledgement advances `acked`, so the streaming send
    // loop can resume sending the moment new credit is available.
    pub resend_notify: Arc<Notify>,
    // Highest number of coded symbols the receiver has confirmed consuming.
    // Monotonic; the send loop keeps at most WINDOW symbols in flight beyond it.
    pub acked: usize,
}

impl SendingState {
    pub fn new(
        local_iblt: RatelessIBLT<RIBLTSymbol>,
        start_time: Instant,
        session_id: String,
    ) -> Self {
        Self {
            local_iblt,
            start_time,
            session_id,
            resend_notify: Arc::new(Notify::new()),
            acked: 0,
        }
    }
}

pub struct ReceivingState {
    pub session_id: String,
    pub start_time: Instant,
    // Feeds incoming coded-symbol batches (start index, symbols) to the
    // per-session decode task that owns the Decoder. Dropping it closes the
    // channel and ends that task.
    pub symbol_tx: mpsc::UnboundedSender<(u64, Vec<RIBLTCodedSymbol>)>,
}

impl ReceivingState {
    pub fn new(
        session_id: String,
        start_time: Instant,
        symbol_tx: mpsc::UnboundedSender<(u64, Vec<RIBLTCodedSymbol>)>,
    ) -> Self {
        Self { session_id, start_time, symbol_tx }
    }
}

pub const RIBLT_PROTOCOL_ID: u64 = 1;
// Coded symbols carried in a single SendSymbol message.
const CHUNK_SIZE: usize = 256;
// Maximum coded symbols the sender keeps in flight (sent but unacknowledged).
// Bounds how far past the ~1.35*d decode point the sender can overshoot.
const SEND_WINDOW: usize = 4096;

pub struct RIBLT {
    id: u64,
    state: Arc<DefaultNodeState>,
    port: NodeAddress,
    deserializer: Arc<RIBLTDeserializer>,
    sending_states: Arc<RwLock<HashMap<NodeAddress, SendingState>>>,
    pub receiving_states: Arc<RwLock<HashMap<NodeAddress, ReceivingState>>>,
}

impl RIBLT {
    pub fn new(state: Arc<DefaultNodeState>, port: NodeAddress) -> Self {
        Self {
            id: RIBLT_PROTOCOL_ID,
            state,
            port,
            deserializer: Arc::new(RIBLTDeserializer::default()),
            sending_states: Arc::new(RwLock::new(HashMap::new())),
            receiving_states: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn sending_symbols_sequence(
        state: Arc<DefaultNodeState>,
        own_address: NodeAddress,
        neighbor_address: NodeAddress,
        protocol_id: u64,
        sending_states: Arc<RwLock<HashMap<NodeAddress, SendingState>>>,
    ) {
        info!(
            "Streaming symbols from {:?} to {:?}",
            own_address, neighbor_address
        );

        // Index of the next coded symbol to generate/send. The receiver
        // acknowledges progress via `acked`; we keep at most SEND_WINDOW symbols
        // in flight (sent - acked) and stop once the session is removed, which
        // happens when a FinishedDecoding arrives.
        let mut sent: usize = 0;

        loop {
            let (acked, resend_notify, session_id) = {
                let guard = sending_states.read().await;
                match guard.get(&neighbor_address) {
                    Some(status) => (
                        status.acked,
                        status.resend_notify.clone(),
                        status.session_id.clone(),
                    ),
                    // Session gone (peer finished decoding) -> stop streaming.
                    None => break,
                }
            };

            let in_flight = sent.saturating_sub(acked);
            if in_flight >= SEND_WINDOW {
                // Window full: wait for an acknowledgement to free up credit.
                // The timeout is a safety net against a lost wake-up.
                let _ = timeout(Duration::from_millis(5000), resend_notify.notified()).await;
                continue;
            }

            let budget = (SEND_WINDOW - in_flight).min(CHUNK_SIZE);
            let start_index = sent;
            let mut symbols = Vec::with_capacity(budget);
            {
                let mut states_guard = sending_states.write().await;
                let status_guard = match states_guard.get_mut(&neighbor_address) {
                    Some(guard) => guard,
                    None => break,
                };
                for i in 0..budget {
                    let coded_symbol = status_guard.local_iblt.get_coded_symbol(start_index + i);
                    symbols.push(RIBLTCodedSymbol {
                        sum: coded_symbol.sum,
                        hash: coded_symbol.hash,
                        count: coded_symbol.count,
                    });
                }
            }
            sent += budget;

            state
                .send_through_socket(
                    own_address.clone(),
                    Box::new(neighbor_address.clone()),
                    Box::new(RIBLTSendSymbolMessage::new(
                        RIBLTMessageType::new(RIBLTMessageTypeValues::SendSymbol),
                        Some(protocol_id),
                        symbols,
                        session_id,
                        start_index as u64,
                    )),
                )
                .await
                .unwrap();
        }

        info!("Stopped streaming to {:?} after {} symbols", neighbor_address, sent);
    }

    async fn reconciliation_mechanism(
        state: Arc<DefaultNodeState>,
        port: NodeAddress,
        protocol_id: u64,
        sending_states: Arc<RwLock<HashMap<NodeAddress, SendingState>>>,
    ) -> Result<(), String> {
        info!("Ran reconciliation mechanism");

        let connection_targets = state.membership().read().await.valid_connection_targets();

        info!("Valid connection targets: {:?}", connection_targets);

        for info in connection_targets {
            if let Some(_) = sending_states.read().await.get(&info) {
                info!(
                    "Already have reconciliation in progress with neighbor {:?}, skipping",
                    info
                );
                continue;
            }

            info!(
                "Initializing neighbor reconciliation for neighbor {:?}",
                info
            );
            Self::init_sending_state(state.clone(), sending_states.clone(), info.clone()).await;
            info!(
                "Finished initializing neighbor reconciliation for neighbor {:?}",
                info
            );

            let state_clone = state.clone();
            let port_clone = port.clone();
            let info_clone = info.clone();
            let protocol_id_clone = protocol_id;
            let sending_states_clone = sending_states.clone();

            info!("Sending symbols sequence to neighbor {:?}", info);
            spawn!({
                RIBLT::sending_symbols_sequence(
                    state_clone,
                    port_clone,
                    info_clone,
                    protocol_id_clone,
                    sending_states_clone,
                )
                .await;
            });
        }

        Ok(())
    }

    pub async fn check_if_already_sending(
        sending_states: Arc<RwLock<HashMap<NodeAddress, SendingState>>>,
        neighbor: NodeAddress,
    ) -> bool {
        sending_states.read().await.contains_key(&neighbor)
    }

    pub async fn init_sending_state(
        state: Arc<DefaultNodeState>,
        sending_states: Arc<RwLock<HashMap<NodeAddress, SendingState>>>,
        neighbor: NodeAddress,
    ) {
        let symbols = session::load_iblt_symbols(&state);
        sending_states.write().await.insert(
            neighbor,
            SendingState::new(
                RatelessIBLT::new(symbols),
                Instant::now(),
                uuid::Uuid::new_v4().to_string(),
            ),
        );
    }
}
