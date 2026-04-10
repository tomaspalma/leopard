use metrics::{counter, histogram};
use runtime::spawn;

use riblt::{symbol::PeelableResult, RatelessIBLT, UnmanagedRatelessIBLT};

use crate::riblt::{
    messages::{RIBLTMessageType, RIBLTMessageTypeValues, RIBLTSymbol},
    ReconciliationNeighborStatus,
};

use connection::{
    node::{id::NodeIdentifier, port::NodeAddress},
    route::RouteTask,
};
use dashmap::DashMap;
use protocol::deserializer::ProtocolDeserializer;
use state::node::{DefaultNodeState, NodeState};
use std::sync::Arc;
use tracing::{error, info};

use crate::riblt::{
    messages::{RIBLTDecodedAllMessage, RIBLTSendSymbolMessage},
    RIBLTDeserializer, RIBLT, RIBLT_PROTOCOL_ID,
};

pub struct ReceiveNeighborSymbolsTask {
    identifier: Arc<dyn NodeIdentifier<NodeAddress, NodeAddress> + Send + Sync>,
    state: Arc<DefaultNodeState>,
    neighbor_states: Arc<DashMap<NodeAddress, crate::riblt::ReconciliationNeighborStatus>>,
}

impl ReceiveNeighborSymbolsTask {
    pub fn new(
        identifier: Arc<dyn NodeIdentifier<NodeAddress, NodeAddress> + Send + Sync>,
        state: Arc<DefaultNodeState>,
        neighbor_states: Arc<DashMap<NodeAddress, crate::riblt::ReconciliationNeighborStatus>>,
    ) -> Self {
        Self {
            identifier,
            state,
            neighbor_states,
        }
    }

    fn receive_symbols_neighbor_decoded(&self, neighbor: NodeAddress) {
        info!("Neighbor successfully decoded symbols");
        self.neighbor_states.remove(&neighbor);
    }

    fn filter_remote_peeled_symbols(
        &self,
        peeled_symbols: Vec<PeelableResult<RIBLTSymbol>>,
    ) -> Vec<RIBLTSymbol> {
        info!("Filtering remote peeled symbols");
        peeled_symbols
            .into_iter()
            .filter_map(|symbol| match symbol {
                PeelableResult::Remote(s) => Some(s),
                _ => None,
            })
            .collect()
    }

    fn apply_reconciliation_result(&self, result: Vec<RIBLTSymbol>) {
        if result.is_empty() {
            info!("No reconciliation result");
        } else {
            info!("Reconciliation result length: {}", result.len());
        }

        let state_clone = self.state.clone();
        spawn!({
            if let Some(storage) = state_clone.get_storage("default".to_string()) {
                for symbol in result {
                    storage
                        .store(Box::new(state::storage::item::DefaultDataStateItem::new(
                            symbol.key,
                            symbol.value,
                        )))
                        .await;
                }
            }
        });
    }

    async fn handle_received_symbols(&self, message: RIBLTSendSymbolMessage, neighbor: NodeAddress) {
        for symbol in message.symbols() {
            let (local_coded_symbols, remote_coded_symbols, mut stored_symbols) =
                match self.neighbor_states.get_mut(&neighbor) {
                    Some(mut status) => {
                        let mut cs = riblt::CodedSymbol::new();
                        cs.sum = symbol.sum.clone();
                        cs.hash = symbol.hash;
                        cs.count = symbol.count;
                        status.remote_iblt.add_coded_symbol(&cs);

                        let coded_symbols_len = status.remote_iblt.coded_symbols.len() as usize;
                        status.local_iblt.extend_coded_symbols(coded_symbols_len);

                        (
                            status.local_iblt.coded_symbols.clone(),
                            status.remote_iblt.coded_symbols.clone(),
                            status.stored_symbols.clone(),
                        )
                    }
                    None => {
                        error!("Failed to get IBLT for neighbor {:?}", neighbor);
                        continue;
                    }
                };

            let neighbor_clone = neighbor.clone();
            
            let (is_peeling_successful, new_symbols) = tokio::task::spawn_blocking(move || {
                let local_iblt = UnmanagedRatelessIBLT {
                    coded_symbols: local_coded_symbols,
                };
                let remote_iblt = UnmanagedRatelessIBLT {
                    coded_symbols: remote_coded_symbols,
                };

                let decode_start = std::time::Instant::now();
                let mut collapsed = local_iblt.collapse(&remote_iblt);
                let peel_symbols = collapsed.peel_all_symbols();
                
                let result = peel_symbols
                    .into_iter()
                    .filter_map(|symbol| match symbol {
                        riblt::symbol::PeelableResult::Remote(s) => Some(s),
                        _ => None,
                    })
                    .collect::<Vec<RIBLTSymbol>>();

                histogram!("riblt_decode_duration_seconds", "neighbor" => format!("{:?}", neighbor_clone))
                    .record(decode_start.elapsed().as_secs_f64());

                let successful = RIBLT::peeling_successful(&mut collapsed);
                (successful, result)
            }).await.unwrap();

            stored_symbols.extend(new_symbols.clone());

            if !new_symbols.is_empty() || is_peeling_successful {
                if let Some(mut status) = self.neighbor_states.get_mut(&neighbor) {
                    status.stored_symbols = stored_symbols.clone();
                }
            }

            if is_peeling_successful {
                info!("Peeling successful for neighbor {:?}", neighbor);

                self.apply_reconciliation_result(stored_symbols.clone());

                let state_clone = self.state.clone();
                let id_clone = self.identifier.connection_info().clone();
                let neighbor_clone = neighbor.clone();
                spawn!({
                    let _ = state_clone
                        .send_through_socket(
                            id_clone,
                            Box::new(neighbor_clone),
                            Box::new(RIBLTDecodedAllMessage::new(
                                RIBLTMessageType::new(RIBLTMessageTypeValues::FinishedDecoding),
                                Some(RIBLT_PROTOCOL_ID),
                            )),
                        )
                        .await;
                });
                break;
            }
        }
    }

    async fn receive_incoming_symbols(
        self: Arc<Self>,
        message: RIBLTSendSymbolMessage,
        neighbor: NodeAddress,
    ) {
        info!("Received RIBLT message");
        counter!("riblt_symbols_received", "neighbor" => format!("{:?}", neighbor))
            .increment(message.symbols().len() as u64);

        info!("Checking if neighbor {:?} is already reconciling", neighbor);
        if !RIBLT::check_if_neighbor_already_reconciling(
            self.neighbor_states.clone(),
            neighbor.clone(),
        ) {
            info!("Initializing neighbor reconciliation for neighbor {:?}", neighbor);
            let state_clone = self.state.clone();
            let neighbor_states_clone = self.neighbor_states.clone();
            let neighbor_clone = neighbor.clone();
            
            tokio::task::spawn_blocking(move || {
                RIBLT::init_neighbor_reconciliation(
                    state_clone,
                    neighbor_states_clone,
                    neighbor_clone,
                );
            }).await.unwrap();
            
            info!("Finished initializing neighbor reconciliation for neighbor {:?}", neighbor);
        }

        self.handle_received_symbols(message, neighbor).await;
    }
}

impl RouteTask for ReceiveNeighborSymbolsTask {
    fn run(self: Arc<Self>, message: Vec<u8>, neighbor: NodeAddress) {
        spawn!({
            let msg_to_process = {
                let deserialized_message = RIBLTDeserializer::new().deserialize(message);
                let msg_type_box = deserialized_message.get_type().value();

                if let Some(riblt_type) = msg_type_box.as_any().downcast_ref::<RIBLTMessageTypeValues>() {
                    match riblt_type {
                        RIBLTMessageTypeValues::SendSymbol => {
                            if let Some(msg) = deserialized_message.as_any().downcast_ref::<RIBLTSendSymbolMessage>() {
                                Some(msg.clone())
                            } else {
                                error!("Failed to downcast message to RIBLTSendSymbolMessage");
                                None
                            }
                        }
                        RIBLTMessageTypeValues::FinishedDecoding => {
                            self.receive_symbols_neighbor_decoded(neighbor.clone());
                            None
                        }
                    }
                } else {
                    error!("Received unexpected message type");
                    None
                }
            };
            
            if let Some(msg) = msg_to_process {
                self.receive_incoming_symbols(msg, neighbor).await;
            }
        });
    }
}
