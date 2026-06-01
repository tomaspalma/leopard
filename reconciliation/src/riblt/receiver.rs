use metrics::{counter, gauge, histogram};
use runtime::metrics::experiment::get_context;
use runtime::spawn;

use riblt::Decoder;

use crate::riblt::{
    messages::{RIBLTMessageType, RIBLTMessageTypeValues},
    session::{add_coded_symbols, store_symbols, try_decode_blocking},
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

    async fn handle_received_symbols(
        &self,
        message: RIBLTSendSymbolMessage,
        neighbor: NodeAddress,
    ) {
        // Add incoming coded symbols and move the decoder out for blocking work.
        let (decoder, remote_cursor) = match self.receiving_states.write().await.get_mut(&neighbor) {
            Some(status) => {
                add_coded_symbols(&mut status.decoder, message.symbols());
                (
                    std::mem::replace(&mut status.decoder, Decoder::new()),
                    status.stored_remote,
                )
            }
            None => {
                error!("Failed to get decoder for neighbor {:?}", neighbor);
                return;
            }
        };

        let decode_start = std::time::Instant::now();
        let (decoder, peel_result) = try_decode_blocking(decoder, remote_cursor).await;

        // Put the decoder back, unless the session was reset while we were working.
        let session_id = message.session_id().clone();
        if let Some(status) = self.receiving_states.write().await.get_mut(&neighbor) {
            if status.session_id == session_id {
                status.decoder = decoder;
                status.stored_remote = peel_result.remote_total;
            }
        }
        histogram!("riblt_decode_duration_seconds", "neighbor" => format!("{:?}", neighbor))
            .record(decode_start.elapsed().as_secs_f64());

        store_symbols(&self.state, peel_result.remote_symbols).await;

        if peel_result.successful {
            info!("Peeling successful for neighbor {:?}", neighbor);
            let context = get_context();

            let round_duration = self
                .receiving_states
                .read()
                .await
                .get(&neighbor)
                .map(|s| s.start_time.elapsed().as_secs_f64());

            if let Some(duration) = round_duration {
                gauge!(
                    "reconciliation_round_duration_seconds",
                    "protocol" => "riblt",
                    "neighbor" => format!("{:?}", neighbor),
                    "run_id" => context.run_id().to_string(),
                    "trial" => context.trial().to_string(),
                    "similarity" => context.similarity().to_string()
                )
                .set(duration);
            }

            counter!(
                "reconciliation_completed",
                "protocol" => "riblt",
                "neighbor" => format!("{:?}", neighbor),
                "run_id" => context.run_id().to_string(),
                "trial" => context.trial().to_string(),
                "similarity" => context.similarity().to_string()
            )
            .increment(1);
            runtime::metrics::csv::finish_iteration(
                format!("{:?}", self.identifier.connection_info()),
                format!("{:?}", neighbor),
                "riblt",
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
            RIBLT::init_receiving_state(
                self.state.clone(),
                self.receiving_states.clone(),
                neighbor.clone(),
                msg_session_id.clone(),
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
                                    status.resend_notify.notify_one();
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
