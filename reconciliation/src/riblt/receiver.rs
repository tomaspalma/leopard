use connection::{node::port::NodeAddress, route::RouteTask};
use dashmap::DashMap;
use protocol::deserializer::ProtocolDeserializer;
use riblt::{RatelessIBLT, UnmanagedRatelessIBLT};
use std::{collections::HashSet, sync::Arc};
use tracing::{error, info};

use crate::riblt::{
    messages::{RIBLTSendSymbolMessage, RIBLTSymbol},
    RIBLTDeserializer, RIBLT,
};

pub struct ReceiveNeighborSymbolsTask {
    riblts: Arc<DashMap<NodeAddress, RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>>>>,
    reconciliation_riblts: Arc<DashMap<NodeAddress, UnmanagedRatelessIBLT<RIBLTSymbol>>>,
}

impl ReceiveNeighborSymbolsTask {
    pub fn new(
        riblts: Arc<DashMap<NodeAddress, RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>>>>,
        reconciliation_riblts: Arc<DashMap<NodeAddress, UnmanagedRatelessIBLT<RIBLTSymbol>>>,
    ) -> Self {
        Self {
            riblts,
            reconciliation_riblts,
        }
    }
}

impl RouteTask for ReceiveNeighborSymbolsTask {
    fn run(&self, message: Vec<u8>, neighbor: NodeAddress) {
        let deserialized_message = RIBLTDeserializer::new().deserialize(message);

        let message = deserialized_message
            .as_any()
            .downcast_ref::<RIBLTSendSymbolMessage>();

        match message {
            Some(msg) => {
                info!("Received RIBLT message");

                if !RIBLT::check_if_neighbor_already_reconciling(
                    self.riblts.clone(),
                    self.reconciliation_riblts.clone(),
                    neighbor.clone(),
                ) {
                    self.reconciliation_riblts
                        .insert(neighbor.clone(), UnmanagedRatelessIBLT::new());
                }

                for symbol in msg.symbols() {
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
                                },
                                None => {
                                    let all_keys: Vec<_> = self.riblts.iter().map(|r| format!("{:?}", r.key())).collect();
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

                            let mut a = local_riblt.collapse(&riblt);

                            let peel_symbols = a.peel_all_symbols();

                            info!("Peel symbols: {:?}", peel_symbols);
                        }
                        None => error!("Failed to get or create IBLT for neighbor {:?}", neighbor),
                    }
                }
            }
            None => error!("Failed to downcast message to RIBLTSendSymbolMessage"),
        }
    }
}
