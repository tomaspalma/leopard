use metrics::{counter, histogram};
use runtime::spawn;

use riblt::{symbol::PeelableResult, RatelessIBLT};

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
        let mut remove = false;
        if let Some(mut status) = self.neighbor_states.get_mut(&neighbor) {
            status.remote_decoded_local = true;
            remove = status.decoded_remote && status.remote_decoded_local;
        }

        if remove {
            self.neighbor_states.remove(&neighbor);
        }
    }

    fn filter_remote_peeled_symbols(
        &self,
        peeled_symbols: Vec<PeelableResult<RIBLTSymbol>>,
    ) -> Vec<RIBLTSymbol> {
        info!("Filtering remote peeled symbols: {:?}", peeled_symbols);
        peeled_symbols
            .into_iter()
            .filter(|symbol| match symbol {
                PeelableResult::Remote(_) => true,
                _ => false,
            })
            .map(|symbol| match symbol {
                PeelableResult::Remote(s) => s,
                _ => unreachable!(),
            })
            .collect()
    }

    fn apply_reconciliation_result(&self, result: Vec<RIBLTSymbol>) {
        match result.len() {
            0 => info!("No reconciliation result"),
            _ => info!("Reconciliation result length: {}", result.len()),
        }

        info!("Applying reconciliation result: {:?}", result);

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

    fn receive_incoming_symbols(&self, message: &RIBLTSendSymbolMessage, neighbor: NodeAddress) {
        info!("Received RIBLT message");
        counter!("riblt_symbols_received", "neighbor" => format!("{:?}", neighbor))
            .increment(message.symbols().len() as u64);

        if !RIBLT::check_if_neighbor_already_reconciling(
            self.neighbor_states.clone(),
            neighbor.clone(),
        ) {
            RIBLT::init_neighbor_reconciliation(
                self.state.clone(),
                self.neighbor_states.clone(),
                neighbor.clone(),
            );
        }

        for symbol in message.symbols() {
            let mut decoded_now = false;
            let mut remove = false;
            match self.neighbor_states.get_mut(&neighbor) {
                Some(mut status) => {
                    if status.decoded_remote {
                        continue;
                    }
                    let mut cs = riblt::CodedSymbol::new();
                    cs.sum = symbol.sum.clone();
                    cs.hash = symbol.hash;
                    cs.count = symbol.count;
                    status.remote_iblt.add_coded_symbol(&cs);

                    let (local_iblt, remote_iblt, start_time) = {
                        let status_ref = &mut *status;
                        (
                            &mut status_ref.local_iblt,
                            &status_ref.remote_iblt,
                            status_ref.start_time,
                        )
                    };

                    let decode_start = std::time::Instant::now();
                    let mut collapsed = local_iblt.collapse(remote_iblt);
                    let peel_symbols = collapsed.peel_all_symbols();

                    histogram!("riblt_decode_duration_seconds", "neighbor" => format!("{:?}", neighbor))
                        .record(decode_start.elapsed().as_secs_f64());

                    info!("Peel symbols: {:?}", peel_symbols);

                    if RIBLT::peeling_successful(&mut collapsed) {
                        info!("Peeling successful for neighbor {:?}", neighbor);
                        status.decoded_remote = true;
                        decoded_now = true;
                        remove = status.decoded_remote && status.remote_decoded_local;

                        let state_clone = self.state.clone();
                        let id_clone = self.identifier.connection_info().clone();
                        let neighbor_clone = neighbor.clone();

                        let remote_peeled = self.filter_remote_peeled_symbols(peel_symbols);
                        let diff_count = remote_peeled.len() as u64;

                        histogram!("riblt_reconciliation_duration_seconds", "neighbor" => format!("{:?}", neighbor), "differences" => diff_count.to_string())
                            .record(start_time.elapsed().as_secs_f64());

                        counter!("riblt_differences_resolved", "neighbor" => format!("{:?}", neighbor))
                            .increment(diff_count);

                        self.apply_reconciliation_result(remote_peeled);

                        spawn!({
                            let _ = state_clone
                                .send_through_socket(
                                    id_clone,
                                    Box::new(neighbor_clone),
                                    Box::new(RIBLTDecodedAllMessage::new(
                                        RIBLTMessageType::new(
                                            RIBLTMessageTypeValues::FinishedDecoding,
                                        ),
                                        Some(RIBLT_PROTOCOL_ID),
                                    )),
                                )
                                .await;
                        });
                    }
                }
                None => error!("Failed to get IBLT for neighbor {:?}", neighbor),
            }
            if remove {
                self.neighbor_states.remove(&neighbor);
            }
            if decoded_now {
                break;
            }
        }
    }
}

impl RouteTask for ReceiveNeighborSymbolsTask {
    fn run(&self, message: Vec<u8>, neighbor: NodeAddress) {
        let deserialized_message = RIBLTDeserializer::new().deserialize(message);

        let msg_type_box = deserialized_message.get_type().value();

        if let Some(riblt_type) = msg_type_box
            .as_any()
            .downcast_ref::<RIBLTMessageTypeValues>()
        {
            match riblt_type {
                RIBLTMessageTypeValues::SendSymbol => {
                    if let Some(msg) = deserialized_message
                        .as_any()
                        .downcast_ref::<RIBLTSendSymbolMessage>()
                    {
                        self.receive_incoming_symbols(msg, neighbor);
                    } else {
                        error!("Failed to downcast message to RIBLTSendSymbolMessage");
                    }
                }
                RIBLTMessageTypeValues::FinishedDecoding => {
                    self.receive_symbols_neighbor_decoded(neighbor);
                }
            }
        } else {
            error!("Received unexpected message type");
        }
    }
}
