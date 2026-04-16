pub mod deserializer;
pub mod messages;
pub mod protocols;
pub mod receiver;

use runtime::spawn;

use deserializer::RIBLTDeserializer;

use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;

use std::sync::Arc;
use tracing::info;

use state::{
    node::{DefaultNodeState, NodeState},
    storage::item::DataStateItem,
};

use connection::node::port::NodeAddress;
use membership::{Membership, MembershipNeighbor, MembershipNeighbors};

use crate::riblt::messages::{
    RIBLTCodedSymbol, RIBLTMessageType, RIBLTMessageTypeValues, RIBLTSendSymbolMessage, RIBLTSymbol,
};
use riblt::{RatelessIBLT, UnmanagedRatelessIBLT};

use metrics::counter;

use tokio::time::{sleep, Duration};

#[derive(Debug, Clone, PartialEq)]
pub enum ReconciliationState {
    SendingSymbols,
    AwaitingConfirmation,
}

use std::time::Instant;

pub struct SendingState {
    pub state: ReconciliationState,
    pub local_iblt: RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>>,
    pub start_time: Instant,
    pub session_id: String,
}

impl SendingState {
    pub fn new(
        state: ReconciliationState,
        local_iblt: RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>>,
        start_time: Instant,
        session_id: String,
    ) -> Self {
        Self {
            state,
            local_iblt,
            start_time,
            session_id,
        }
    }
}

pub struct ReceivingState {
    pub local_iblt: RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>>,
    pub remote_iblt: UnmanagedRatelessIBLT<RIBLTSymbol>,
    pub start_time: Instant,
    pub session_id: String,
}

impl ReceivingState {
    pub fn new(
        local_iblt: RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>>,
        remote_iblt: UnmanagedRatelessIBLT<RIBLTSymbol>,
        start_time: Instant,
        session_id: String,
    ) -> Self {
        Self {
            local_iblt,
            remote_iblt,
            start_time,
            session_id,
        }
    }
}

pub const RIBLT_PROTOCOL_ID: u64 = 1;
const BATCH_SIZE: usize = 5;

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

    pub fn peeling_successful(riblt: &mut UnmanagedRatelessIBLT<RIBLTSymbol>) -> bool {
        riblt.is_empty()
    }

    async fn get_neighbor_sending_state_status(
        neighbor: &NodeAddress,
        sending_states: &Arc<RwLock<HashMap<NodeAddress, SendingState>>>,
    ) -> Option<ReconciliationState> {
        let lock = sending_states.read().await;
        if let Some(status) = lock.get(&neighbor) {
            Some(status.state.clone())
        } else {
            None
        }
    }

    fn update_symbols(
        symbols: &mut HashSet<RIBLTSymbol>,
        items: Vec<Box<dyn DataStateItem + Send + Sync>>,
    ) {
        for item in items {
            symbols.insert(RIBLTSymbol {
                key: item.key().to_string(),
                value: item.value().to_string(),
            });
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
            "Running sending symbols sequence from {:?} to {:?}",
            own_address, neighbor_address
        );

        let mut current_index = 0;
        let mut wait_time_ms = 0;

        if !sending_states.read().await.contains_key(&neighbor_address) {
            info!(
                "Neighbor {:?} not found in neighbor states, aborting sending symbols sequence",
                neighbor_address
            );
            return;
        }

        while sending_states
            .clone()
            .read()
            .await
            .contains_key(&neighbor_address)
        {
            if Self::get_neighbor_sending_state_status(&neighbor_address, &sending_states)
                .await
                .unwrap()
                == ReconciliationState::AwaitingConfirmation
            {
                sleep(Duration::from_millis(100)).await;
                wait_time_ms += 100;

                if wait_time_ms >= 5000 {
                    info!("Timeout waiting for confirmation from {:?}, reverting to SendingSymbols to send next batch", neighbor_address);
                    if let Some(status) = sending_states.write().await.get_mut(&neighbor_address) {
                        status.state = ReconciliationState::SendingSymbols;
                    }
                    wait_time_ms = 0;
                }

                continue;
            }

            wait_time_ms = 0;

            let mut symbols = Vec::new();
            let session_id;

            {
                let mut states_guard = sending_states.write().await;
                let status_guard = match states_guard.get_mut(&neighbor_address) {
                    Some(guard) => guard,
                    None => break,
                };

                session_id = status_guard.session_id.clone();

                for _ in 0..BATCH_SIZE {
                    let coded_symbol = status_guard.local_iblt.get_coded_symbol(current_index);

                    let symbol_message = RIBLTCodedSymbol {
                        sum: coded_symbol.sum,
                        hash: coded_symbol.hash,
                        count: coded_symbol.count,
                    };
                    symbols.push(symbol_message);

                    current_index += 1;
                }

                status_guard.state = ReconciliationState::AwaitingConfirmation;
            }

            let symbols_len = symbols.len() as u64;

            state
                .send_through_socket(
                    own_address.clone(),
                    Box::new(neighbor_address.clone()),
                    Box::new(RIBLTSendSymbolMessage::new(
                        RIBLTMessageType::new(RIBLTMessageTypeValues::SendSymbol),
                        Some(protocol_id),
                        symbols,
                        session_id,
                    )),
                )
                .await
                .unwrap();

            counter!("riblt_symbols_sent", "neighbor" => format!("{:?}", neighbor_address))
                .increment(symbols_len);

            info!(
                "Sent batch of {} symbols up to index {}",
                BATCH_SIZE, current_index
            );
        }
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
        let mut symbols = HashSet::new();
        if let Some(storage) = state.get_storage("default".to_string()) {
            for item in storage.items() {
                symbols.insert(RIBLTSymbol {
                    key: item.key().to_string(),
                    value: item.value().to_string(),
                });
            }
        }

        sending_states.write().await.insert(
            neighbor,
            SendingState::new(
                ReconciliationState::SendingSymbols,
                RatelessIBLT::new(symbols),
                Instant::now(),
                uuid::Uuid::new_v4().to_string(),
            ),
        );
    }

    pub async fn init_receiving_state(
        state: Arc<DefaultNodeState>,
        receiving_states: Arc<RwLock<HashMap<NodeAddress, ReceivingState>>>,
        neighbor: NodeAddress,
        session_id: String,
    ) {
        let mut symbols = HashSet::new();
        if let Some(storage) = state.get_storage("default".to_string()) {
            for item in storage.items() {
                symbols.insert(RIBLTSymbol {
                    key: item.key().to_string(),
                    value: item.value().to_string(),
                });
            }
        }

        receiving_states.write().await.insert(
            neighbor,
            ReceivingState::new(
                RatelessIBLT::new(symbols),
                UnmanagedRatelessIBLT::new(),
                Instant::now(),
                session_id,
            ),
        );
    }
}
