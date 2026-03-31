use runtime::spawn;

use crate::riblt::messages::{RIBLTMessageType, RIBLTMessageTypeValues};

use connection::{
    node::{id::NodeIdentifier, port::NodeAddress},
    route::RouteTask,
};
use dashmap::DashMap;
use protocol::deserializer::ProtocolDeserializer;
use riblt::{RatelessIBLT, UnmanagedRatelessIBLT};
use state::node::{DefaultNodeState, NodeState};
use std::{collections::HashSet, sync::Arc};
use tracing::{error, info};

use crate::riblt::{
    messages::{RIBLTDecodedAllMessage, RIBLTSendSymbolMessage, RIBLTSymbol},
    RIBLTDeserializer, RIBLT, RIBLT_PROTOCOL_ID,
};

pub struct ReceiveNeighborSymbolsTask {
    identifier: Arc<dyn NodeIdentifier<NodeAddress, NodeAddress> + Send + Sync>,
    state: Arc<DefaultNodeState>,
    riblts: Arc<DashMap<NodeAddress, RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>>>>,
    reconciliation_riblts: Arc<DashMap<NodeAddress, UnmanagedRatelessIBLT<RIBLTSymbol>>>,
}

impl ReceiveNeighborSymbolsTask {
    pub fn new(
        identifier: Arc<dyn NodeIdentifier<NodeAddress, NodeAddress> + Send + Sync>,
        state: Arc<DefaultNodeState>,
        riblts: Arc<DashMap<NodeAddress, RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>>>>,
        reconciliation_riblts: Arc<DashMap<NodeAddress, UnmanagedRatelessIBLT<RIBLTSymbol>>>,
    ) -> Self {
        Self {
            identifier,
            state,
            riblts,
            reconciliation_riblts,
        }
    }

    fn receive_incoming_symbols(&self, message: &RIBLTSendSymbolMessage, neighbor: NodeAddress) {
        info!("Received RIBLT message");

        if !RIBLT::check_if_neighbor_already_reconciling(
            self.riblts.clone(),
            self.reconciliation_riblts.clone(),
            neighbor.clone(),
        ) {
            self.reconciliation_riblts
                .insert(neighbor.clone(), UnmanagedRatelessIBLT::new());
        }

        for symbol in message.symbols() {
            match self.reconciliation_riblts.get_mut(&neighbor) {
                Some(mut riblt) => {
                    let mut cs = riblt::CodedSymbol::new();
                    cs.sum = symbol.sum.clone();
                    cs.hash = symbol.hash;
                    cs.count = symbol.count;
                    riblt.add_coded_symbol(&cs);

                    let mut local_riblt = match self.riblts.get_mut(&neighbor) {
                        Some(guard) => {
                            info!("Found local IBLT for neighbor {:?}", neighbor);
                            guard
                        }
                        None => {
                            let all_keys: Vec<_> = self
                                .riblts
                                .iter()
                                .map(|r| format!("{:?}", r.key()))
                                .collect();
                            panic!(
                                "Key {:?} not found! \nAvailable keys: {:?} \nNeighbor Hash: {:?}",
                                neighbor,
                                all_keys,
                                {
                                    use std::hash::{Hash, Hasher};
                                    let mut s = std::collections::hash_map::DefaultHasher::new();
                                    neighbor.hash(&mut s);
                                    s.finish()
                                }
                            );
                        }
                    };

                    let mut collapsed = local_riblt.collapse(&riblt);
                    let peel_symbols = collapsed.peel_all_symbols();

                    info!("Peel symbols: {:?}", peel_symbols);

                    if RIBLT::peeling_successful(&mut collapsed) {
                        info!("Peeling successful for neighbor {:?}", neighbor);

                        let state_clone = self.state.clone();
                        let id_clone = self.identifier.connection_info().clone();
                        let neighbor_clone = neighbor.clone();

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
                None => error!("Failed to get or create IBLT for neighbor {:?}", neighbor),
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
                    info!("Received FinishedDecoding message from {:?}", neighbor);
                }
            }
        } else {
            error!("Received unexpected message type");
        }
    }
}
