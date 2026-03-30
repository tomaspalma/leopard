pub mod messages;
pub mod protocols;
pub mod receiver;

use runtime::spawn;

use protocol::deserializer::ProtocolDeserializer;

use tracing::error;

use dashmap::DashMap;

use message::Message;
use std::sync::Arc;
use tracing::info;

use state::{
    node::{DefaultNodeState, NodeState},
    storage::item::DataStateItem,
};

use connection::{
    node::port::NodeAddress,
    request::handler::default::{TestMessage, TestMessageType},
};
use membership::{Membership, MembershipNeighbor, MembershipNeighbors};

use crate::riblt::messages::{
    RIBLTCodedSymbol, RIBLTMessageType, RIBLTSendSymbolMessage, RIBLTSymbol,
};
use riblt::{RatelessIBLT, UnmanagedRatelessIBLT};
use rkyv::{from_bytes, rancor::Error};
use std::collections::HashSet;
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone, PartialEq)]
pub enum ReconciliationState {
    SendingSymbols,
    AwaitingConfirmation,
}

const RIBLT_PROTOCOL_ID: u64 = 1;
const BATCH_SIZE: usize = 5;
const BATCH_INTERVAL: Duration = Duration::from_millis(5000);

pub struct RIBLT {
    id: u64,
    state: Arc<DefaultNodeState>,
    port: NodeAddress,
    deserializer: Arc<RIBLTDeserializer>,
    reconciliation_states: Arc<DashMap<NodeAddress, ReconciliationState>>,
    iblt: Arc<DashMap<NodeAddress, RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>>>>,
    reconciliation_riblts: Arc<DashMap<NodeAddress, UnmanagedRatelessIBLT<RIBLTSymbol>>>,
}

impl RIBLT {
    pub fn new(state: Arc<DefaultNodeState>, port: NodeAddress) -> Self {
        Self {
            id: RIBLT_PROTOCOL_ID,
            state,
            port,
            deserializer: Arc::new(RIBLTDeserializer::default()),
            reconciliation_states: Arc::new(DashMap::new()),
            iblt: Arc::new(DashMap::new()),
            reconciliation_riblts: Arc::new(DashMap::new()),
        }
    }

    fn update_symbols(
        symbols: &mut HashSet<RIBLTSymbol>,
        items: Vec<Box<dyn DataStateItem + Send + Sync>>,
    ) {
        for item in items {
            symbols.insert(RIBLTSymbol {
                key: item.key().to_string(),
                value: item.value().as_bytes().to_vec(),
            });
        }
    }

    async fn sending_symbols_sequence(
        state: Arc<DefaultNodeState>,
        own_address: NodeAddress,
        neighbor_address: NodeAddress,
        protocol_id: u64,
        reconciliation_states: Arc<DashMap<NodeAddress, ReconciliationState>>,
        iblt: Arc<DashMap<NodeAddress, RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>>>>,
    ) {
        info!(
            "Running sending symbols sequence from {:?} to {:?}",
            own_address, neighbor_address
        );

        let mut current_index = 0;

        while reconciliation_states.contains_key(&neighbor_address) {
            let mut symbols = Vec::new();

            {
                let mut iblt_guard = match iblt.get_mut(&neighbor_address) {
                    Some(guard) => guard,
                    None => break,
                };

                for _ in 0..BATCH_SIZE {
                    let coded_symbol = iblt_guard.get_coded_symbol(current_index);

                    let symbol_message = RIBLTCodedSymbol {
                        sum: coded_symbol.sum,
                        hash: coded_symbol.hash,
                        count: coded_symbol.count,
                    };
                    symbols.push(symbol_message);

                    current_index += 1;
                }
            }

            state
                .send_through_socket(
                    own_address.clone(),
                    Box::new(neighbor_address.clone()),
                    Box::new(RIBLTSendSymbolMessage::new(
                        RIBLTMessageType::new(),
                        Some(protocol_id),
                        symbols,
                    )),
                )
                .await
                .unwrap();

            info!(
                "Sent batch of {} symbols up to index {}",
                BATCH_SIZE, current_index
            );

            reconciliation_states
                .get_mut(&neighbor_address)
                .map(|mut state| {
                    *state = ReconciliationState::AwaitingConfirmation;
                });

            sleep(BATCH_INTERVAL).await;
        }
    }

    async fn reconciliation_mechanism(
        state: Arc<DefaultNodeState>,
        port: NodeAddress,
        protocol_id: u64,
        reconciliation_states: Arc<DashMap<NodeAddress, ReconciliationState>>,
        iblt: Arc<DashMap<NodeAddress, RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>>>>,
    ) -> Result<(), String> {
        info!("Ran reconciliation mechanism");

        let connection_targets = {
            let membership_arc = state.membership();
            let membership_guard = membership_arc.read().unwrap();

            let neighbors_arc = membership_guard.representation().neighbors();
            let neighbors_guard = neighbors_arc.read().unwrap();

            neighbors_guard
                .iter()
                .map(|n| n.read().unwrap())
                .filter(|n| !n.tainted())
                .map(|n| n.identifier().connection_info())
                .collect::<Vec<_>>()
        };

        for info in connection_targets {
            if let Some(_) = reconciliation_states.get(&info) {
                continue;
            }

            reconciliation_states.insert(info.clone(), ReconciliationState::SendingSymbols);

            if !iblt.contains_key(&info) {
                let mut symbols = HashSet::new();
                if let Some(storage) = state.get_storage("default".to_string()) {
                    for item in storage.items() {
                        symbols.insert(RIBLTSymbol {
                            key: item.key().to_string(),
                            value: item.value().as_bytes().to_vec(),
                        });
                    }
                }
                iblt.insert(info.clone(), RatelessIBLT::new(symbols));
            }

            let state_clone = state.clone();
            let port_clone = port.clone();
            let info_clone = info.clone();
            let protocol_id_clone = protocol_id;
            let reconciliation_states_clone = reconciliation_states.clone();
            let iblt_clone = iblt.clone();

            spawn!({
                RIBLT::sending_symbols_sequence(
                    state_clone,
                    port_clone,
                    info_clone,
                    protocol_id_clone,
                    reconciliation_states_clone,
                    iblt_clone,
                )
                .await;
            });
        }

        Ok(())
    }

    fn check_if_neighbor_already_reconciling(
        riblts: Arc<DashMap<NodeAddress, RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>>>>,
        reconciliation_riblts: Arc<DashMap<NodeAddress, UnmanagedRatelessIBLT<RIBLTSymbol>>>,
        neighbor: NodeAddress,
    ) -> bool {
        reconciliation_riblts.contains_key(&neighbor) && riblts.contains_key(&neighbor)
    }
}

#[derive(Default)]
pub struct RIBLTDeserializer {}

impl RIBLTDeserializer {
    pub fn new() -> Self {
        Self {}
    }
}

impl ProtocolDeserializer for RIBLTDeserializer {
    fn deserialize(&self, bytes: Vec<u8>) -> Arc<dyn Message> {
        if bytes.len() < 16 {
            return Arc::new(TestMessage::new(Arc::new(TestMessageType::new()), None));
        }

        let payload = &bytes[16..];

        match from_bytes::<RIBLTSendSymbolMessage, Error>(payload) {
            Ok(msg) => Arc::new(msg),
            Err(e) => {
                error!("Failed to deserialize RIBLT message: {}", e);
                Arc::new(TestMessage::new(Arc::new(TestMessageType::new()), None))
            }
        }
    }
}
