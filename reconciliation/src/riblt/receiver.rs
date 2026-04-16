use metrics::{counter, gauge, histogram};
use runtime::spawn;

use riblt::{symbol::PeelableResult, UnmanagedRatelessIBLT};

use crate::riblt::{
    messages::{RIBLTMessageType, RIBLTMessageTypeValues, RIBLTSymbol},
    {ReceivingState, SendingState},
};

use connection::{
    node::{id::NodeIdentifier, port::NodeAddress},
    route::RouteTask,
};
use protocol::deserializer::ProtocolDeserializer;
use state::node::{DefaultNodeState, NodeState};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};

use crate::riblt::{
    messages::{RIBLTDecodedAllMessage, RIBLTRequestMoreSymbolsMessage, RIBLTSendSymbolMessage},
    RIBLTDeserializer, ReconciliationState, RIBLT, RIBLT_PROTOCOL_ID,
};

pub struct ReceiveNeighborSymbolsTask {
    identifier: Arc<dyn NodeIdentifier<NodeAddress, NodeAddress> + Send + Sync>,
    state: Arc<DefaultNodeState>,
    sending_states: Arc<RwLock<HashMap<NodeAddress, SendingState>>>,
    receiving_states: Arc<RwLock<HashMap<NodeAddress, ReceivingState>>>,
}

impl ReceiveNeighborSymbolsTask {
    pub fn new(
        identifier: Arc<dyn NodeIdentifier<NodeAddress, NodeAddress> + Send + Sync>,
        state: Arc<DefaultNodeState>,
        sending_states: Arc<RwLock<HashMap<NodeAddress, SendingState>>>,
        receiving_states: Arc<RwLock<HashMap<NodeAddress, ReceivingState>>>,
    ) -> Self {
        Self {
            identifier,
            state,
            sending_states,
            receiving_states,
        }
    }

    async fn receive_symbols_neighbor_decoded(&self, neighbor: NodeAddress, session_id: String) {
        info!("Neighbor successfully decoded symbols");

        let should_remove = self
            .sending_states
            .read()
            .await
            .get(&neighbor)
            .map_or(false, |state| state.session_id == session_id);

        if should_remove {
            self.sending_states.write().await.remove(&neighbor);
        } else if self.sending_states.read().await.contains_key(&neighbor) {
            info!(
                "Session ID mismatch, ignoring FinishedDecoding for neighbor {:?}",
                neighbor
            );
        }
    }

    fn filter_remote_peeled_symbols(
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

    async fn handle_received_symbols(
        &self,
        message: RIBLTSendSymbolMessage,
        neighbor: NodeAddress,
    ) {
        let (local_coded_symbols, remote_coded_symbols) =
            match self.receiving_states.write().await.get_mut(&neighbor) {
                Some(status) => {
                    for symbol in message.symbols() {
                        let mut cs = riblt::CodedSymbol::new();
                        cs.sum = symbol.sum.clone();
                        cs.hash = symbol.hash;
                        cs.count = symbol.count;
                        status.remote_iblt.add_coded_symbol(&cs);
                    }

                    let coded_symbols_len = status.remote_iblt.coded_symbols.len() as usize;
                    status.local_iblt.extend_coded_symbols(coded_symbols_len);

                    (
                        status.local_iblt.coded_symbols.clone(),
                        status.remote_iblt.coded_symbols.clone(),
                    )
                }
                None => {
                    error!("Failed to get IBLT for neighbor {:?}", neighbor);
                    return;
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

            info!("Peel symbols: {:?}", peel_symbols);
            
            let result = Self::filter_remote_peeled_symbols(peel_symbols);
            info!("Result: {:?}", result);

            histogram!("riblt_decode_duration_seconds", "neighbor" => format!("{:?}", neighbor_clone))
                .record(decode_start.elapsed().as_secs_f64());

            let successful = RIBLT::peeling_successful(&mut collapsed);
            (successful, result)
        }).await.unwrap();

        let differences_found = !new_symbols.is_empty();
        self.apply_reconciliation_result(new_symbols);

        if is_peeling_successful {
            info!("Peeling successful for neighbor {:?}", neighbor);
            runtime::metrics::csv::finish_iteration(format!("{:?}", neighbor));
            gauge!("reconciliation_had_differences", "target" => format!("{:?}", neighbor))
                .set(if differences_found { 1.0 } else { 0.0 });

            {
                self.receiving_states.write().await.remove(&neighbor);
            }

            let state_clone = self.state.clone();
            let id_clone = self.identifier.connection_info().clone();
            let neighbor_clone = neighbor.clone();
            let session_id = message.session_id().clone();
            spawn!({
                let _ = state_clone
                    .send_through_socket(
                        id_clone,
                        Box::new(neighbor_clone),
                        Box::new(RIBLTDecodedAllMessage::new(
                            RIBLTMessageType::new(RIBLTMessageTypeValues::FinishedDecoding),
                            Some(RIBLT_PROTOCOL_ID),
                            session_id,
                        )),
                    )
                    .await;
            });
        } else {
            info!(
                "Peeling unsuccessful for neighbor {:?}, requesting more symbols",
                neighbor
            );

            let state_clone = self.state.clone();
            let id_clone = self.identifier.connection_info().clone();
            let neighbor_clone = neighbor.clone();
            let session_id = message.session_id().clone();
            spawn!({
                let _ = state_clone
                    .send_through_socket(
                        id_clone,
                        Box::new(neighbor_clone),
                        Box::new(RIBLTRequestMoreSymbolsMessage::new(
                            RIBLTMessageType::new(RIBLTMessageTypeValues::RequestMoreSymbols),
                            Some(RIBLT_PROTOCOL_ID),
                            session_id,
                        )),
                    )
                    .await;
            });
        }
    }

    async fn receive_incoming_symbols(
        &self,
        message: RIBLTSendSymbolMessage,
        neighbor: NodeAddress,
    ) {
        info!("Received RIBLT message");
        counter!("riblt_symbols_received", "neighbor" => format!("{:?}", neighbor))
            .increment(message.symbols().len() as u64);

        let msg_session_id = message.session_id().clone();

        let should_remove = self.receiving_states.read().await.get(&neighbor).map_or(false, |status| {
            if msg_session_id != status.session_id {
                info!("Session ID mismatch. Expected: {}, Got: {}. Dropping old state and creating new one.", status.session_id, msg_session_id);
                true
            } else {
                false
            }
        });

        if should_remove {
            self.receiving_states.write().await.remove(&neighbor);
        }

        info!("Checking if neighbor {:?} is already reconciling", neighbor);
        if !self.receiving_states.read().await.contains_key(&neighbor) {
            info!(
                "Initializing neighbor reconciliation for neighbor {:?}",
                neighbor
            );
            let state_clone = self.state.clone();
            let receiving_states_clone = self.receiving_states.clone();
            let neighbor_clone = neighbor.clone();
            let session_id_clone = msg_session_id.clone();

            RIBLT::init_receiving_state(
                state_clone,
                receiving_states_clone,
                neighbor_clone,
                session_id_clone,
            )
            .await;

            info!(
                "Finished initializing neighbor reconciliation for neighbor {:?}",
                neighbor
            );
        }

        self.handle_received_symbols(message, neighbor).await;
    }
}

impl RouteTask for ReceiveNeighborSymbolsTask {
    fn run(self: Arc<Self>, message: Vec<u8>, neighbor: NodeAddress) {
        let deserialized_message = RIBLTDeserializer::new().deserialize(message);

        let msg_type_box = deserialized_message.get_type().value();

        let riblt_type = msg_type_box
            .as_any()
            .downcast_ref::<RIBLTMessageTypeValues>()
            .cloned();

        let this = self.clone();

        spawn!({
            if let Some(msg_enum) = riblt_type {
                match msg_enum {
                    RIBLTMessageTypeValues::SendSymbol => {
                        info!("Received SendSymbol from {:?}", neighbor);
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RIBLTSendSymbolMessage>()
                        {
                            this.receive_incoming_symbols(msg.clone(), neighbor).await;
                        } else {
                            error!("Failed to downcast message to RIBLTSendSymbolMessage");
                        }
                    }
                    RIBLTMessageTypeValues::FinishedDecoding => {
                        info!("Received FinishedDecoding from {:?}", neighbor);
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RIBLTDecodedAllMessage>()
                        {
                            this.receive_symbols_neighbor_decoded(
                                neighbor,
                                msg.session_id().clone(),
                            )
                            .await;
                        } else {
                            error!("Failed to downcast message to RIBLTDecodedAllMessage");
                        }
                    }
                    RIBLTMessageTypeValues::RequestMoreSymbols => {
                        info!("Received RequestMoreSymbols from {:?}", neighbor);

                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RIBLTRequestMoreSymbolsMessage>()
                        {
                            if let Some(status) =
                                this.sending_states.write().await.get_mut(&neighbor)
                            {
                                if status.session_id == *msg.session_id() {
                                    info!("Status found for {:?} with matching session_id, setting state to SendingSymbols", neighbor);
                                    status.state = ReconciliationState::SendingSymbols;
                                } else {
                                    info!(
                                        "Status found for {:?}, but session_id mismatched",
                                        neighbor
                                    );
                                }
                            } else {
                                info!("No status found for neighbor {:?}", neighbor);
                            }
                        } else {
                            error!("Failed to downcast message to RIBLTRequestMoreSymbolsMessage");
                        }
                    }
                }
            }
        });
    }
}
