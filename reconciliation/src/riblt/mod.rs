pub mod deserializer;
pub mod messages;
pub mod protocols;
pub mod receiver;

use runtime::spawn;

use deserializer::RIBLTDeserializer;

use dashmap::DashMap;

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
use std::collections::HashSet;
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone, PartialEq)]
pub enum ReconciliationState {
    SendingSymbols,
    AwaitingConfirmation,
}

use std::time::Instant;

pub struct ReconciliationNeighborStatus {
    pub state: ReconciliationState,
    pub local_iblt: RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>>,
    pub remote_iblt: UnmanagedRatelessIBLT<RIBLTSymbol>,
    pub start_time: Instant,
}

pub const RIBLT_PROTOCOL_ID: u64 = 1;
const BATCH_SIZE: usize = 5;
const BATCH_INTERVAL: Duration = Duration::from_millis(5000);

pub struct RIBLT {
    id: u64,
    state: Arc<DefaultNodeState>,
    port: NodeAddress,
    deserializer: Arc<RIBLTDeserializer>,
    neighbor_states: Arc<DashMap<NodeAddress, ReconciliationNeighborStatus>>,
}

impl RIBLT {
    pub fn new(state: Arc<DefaultNodeState>, port: NodeAddress) -> Self {
        Self {
            id: RIBLT_PROTOCOL_ID,
            state,
            port,
            deserializer: Arc::new(RIBLTDeserializer::default()),
            neighbor_states: Arc::new(DashMap::new()),
        }
    }

    pub fn peeling_successful(riblt: &mut UnmanagedRatelessIBLT<RIBLTSymbol>) -> bool {
        riblt.is_empty()
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
        neighbor_states: Arc<DashMap<NodeAddress, ReconciliationNeighborStatus>>,
    ) {
        info!(
            "Running sending symbols sequence from {:?} to {:?}",
            own_address, neighbor_address
        );

        let mut current_index = 0;

        while neighbor_states.contains_key(&neighbor_address) {
            let mut symbols = Vec::new();

            {
                let mut status_guard = match neighbor_states.get_mut(&neighbor_address) {
                    Some(guard) => guard,
                    None => break,
                };

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
                    )),
                )
                .await
                .unwrap();

            counter!("riblt_symbols_sent", "neighbor" => format!("{:?}", neighbor_address)).increment(symbols_len);

            info!(
                "Sent batch of {} symbols up to index {}",
                BATCH_SIZE, current_index
            );

            neighbor_states
                .get_mut(&neighbor_address)
                .map(|mut status| {
                    status.state = ReconciliationState::AwaitingConfirmation;
                });

            sleep(BATCH_INTERVAL).await;
        }
    }

    async fn reconciliation_mechanism(
        state: Arc<DefaultNodeState>,
        port: NodeAddress,
        protocol_id: u64,
        neighbor_states: Arc<DashMap<NodeAddress, ReconciliationNeighborStatus>>,
    ) -> Result<(), String> {
        info!("Ran reconciliation mechanism");

        let connection_targets = state.membership().read().unwrap().valid_connection_targets();

        for info in connection_targets {
            if let Some(_) = neighbor_states.get(&info) {
                continue;
            }

            let mut symbols = HashSet::new();
            if let Some(storage) = state.get_storage("default".to_string()) {
                for item in storage.items() {
                    symbols.insert(RIBLTSymbol {
                        key: item.key().to_string(),
                        value: item.value().to_string(),
                    });
                }
            }

            neighbor_states.insert(
                info.clone(),
                ReconciliationNeighborStatus {
                    state: ReconciliationState::SendingSymbols,
                    local_iblt: RatelessIBLT::new(symbols),
                    remote_iblt: UnmanagedRatelessIBLT::new(),
                    start_time: Instant::now(),
                },
            );

            let state_clone = state.clone();
            let port_clone = port.clone();
            let info_clone = info.clone();
            let protocol_id_clone = protocol_id;
            let neighbor_states_clone = neighbor_states.clone();

            spawn!({
                RIBLT::sending_symbols_sequence(
                    state_clone,
                    port_clone,
                    info_clone,
                    protocol_id_clone,
                    neighbor_states_clone,
                )
                .await;
            });
        }

        Ok(())
    }

    pub fn check_if_neighbor_already_reconciling(
        neighbor_states: Arc<DashMap<NodeAddress, ReconciliationNeighborStatus>>,
        neighbor: NodeAddress,
    ) -> bool {
        neighbor_states.contains_key(&neighbor)
    }
}
