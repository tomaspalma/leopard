use connection::{node::port::NodeAddress, route::RouteTask};
use dashmap::DashMap;
use protocol::deserializer::ProtocolDeserializer;
use riblt::UnmanagedRatelessIBLT;
use std::sync::Arc;
use tracing::{error, info};

use crate::riblt::{
    messages::{RIBLTSendSymbolMessage, RIBLTSymbol},
    RIBLTDeserializer, RIBLT,
};

pub struct ReceiveNeighborSymbolsTask {
    reconciliation_riblts: Arc<DashMap<NodeAddress, UnmanagedRatelessIBLT<RIBLTSymbol>>>,
}

impl ReceiveNeighborSymbolsTask {
    pub fn new(
        reconciliation_riblts: Arc<DashMap<NodeAddress, UnmanagedRatelessIBLT<RIBLTSymbol>>>,
    ) -> Self {
        Self {
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
                info!("Received RIBLT message: {:?}", msg);

                if !RIBLT::check_if_neighbor_already_reconciling(
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
                        }
                        None => error!("Failed to get or create IBLT for neighbor {:?}", neighbor),
                    }
                }
            }
            None => error!("Failed to downcast message to RIBLTSendSymbolMessage"),
        }
    }
}
