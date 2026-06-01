use runtime::metrics::experiment::get_context;
use runtime::spawn;

use connection::{node::port::NodeAddress, route::RouteTask};
use protocol::deserializer::ProtocolDeserializer;
use std::sync::Arc;
use tracing::{error, info};

use crate::riblt::{
    messages::{
        RIBLTDecodedAllMessage, RIBLTMessageTypeValues, RIBLTRequestMoreSymbolsMessage,
        RIBLTSendSymbolMessage,
    },
    stream::RibltStreamEngine,
    RIBLTDeserializer,
};

/// Route task for the standalone RIBLT protocol: deserializes incoming messages
/// and forwards them to the shared streaming engine.
pub struct ReceiveNeighborSymbolsTask {
    engine: Arc<RibltStreamEngine>,
}

impl ReceiveNeighborSymbolsTask {
    pub fn new(engine: Arc<RibltStreamEngine>) -> Self {
        Self { engine }
    }
}

impl RouteTask for ReceiveNeighborSymbolsTask {
    fn run(self: Arc<Self>, message: Vec<u8>, neighbor: NodeAddress) {
        let deserialized_message = RIBLTDeserializer::new().deserialize(message);

        let riblt_type = deserialized_message
            .get_type()
            .value()
            .as_any()
            .downcast_ref::<RIBLTMessageTypeValues>()
            .cloned();

        let engine = self.engine.clone();

        spawn!({
            if let Some(msg_enum) = riblt_type {
                let context = get_context();
                metrics::counter!(
                    "protocol_round_trip_count",
                    "target" => format!("{:?}", neighbor),
                    "protocol" => "riblt",
                    "run_id" => context.run_id().to_string(),
                    "trial" => context.trial().to_string(),
                    "similarity" => context.similarity().to_string()
                )
                .increment(1);

                match msg_enum {
                    RIBLTMessageTypeValues::SendSymbol => {
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RIBLTSendSymbolMessage>()
                        {
                            engine
                                .on_symbols(
                                    neighbor,
                                    msg.session_id().clone(),
                                    msg.start_index(),
                                    msg.symbols().clone(),
                                )
                                .await;
                        } else {
                            error!("Failed to downcast message to RIBLTSendSymbolMessage");
                        }
                    }
                    RIBLTMessageTypeValues::FinishedDecoding => {
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RIBLTDecodedAllMessage>()
                        {
                            engine.on_finished(&neighbor, msg.session_id()).await;
                        } else {
                            error!("Failed to downcast message to RIBLTDecodedAllMessage");
                        }
                    }
                    RIBLTMessageTypeValues::RequestMoreSymbols => {
                        if let Some(msg) = deserialized_message
                            .as_any()
                            .downcast_ref::<RIBLTRequestMoreSymbolsMessage>()
                        {
                            engine
                                .on_request_more(
                                    &neighbor,
                                    msg.session_id(),
                                    msg.received_count(),
                                )
                                .await;
                        } else {
                            error!("Failed to downcast message to RIBLTRequestMoreSymbolsMessage");
                        }
                    }
                }
            } else {
                info!("Unknown RIBLT message type");
            }
        });
    }
}
