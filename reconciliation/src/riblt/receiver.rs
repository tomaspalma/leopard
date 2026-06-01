use metrics::{counter, gauge, histogram};
use runtime::metrics::experiment::get_context;
use runtime::spawn;

use crate::riblt::{
    messages::{RIBLTCodedSymbol, RIBLTMessageType, RIBLTMessageTypeValues},
    session::{build_decoder_blocking, load_iblt_symbols, process_batch_blocking, store_symbols},
    {ReceivingState, SendingState},
};

use connection::{
    node::{id::NodeIdentifier, port::NodeAddress},
    route::RouteTask,
};
use protocol::deserializer::ProtocolDeserializer;
use state::node::{DefaultNodeState, NodeState};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info};

use crate::riblt::{
    messages::{RIBLTDecodedAllMessage, RIBLTRequestMoreSymbolsMessage, RIBLTSendSymbolMessage},
    RIBLTDeserializer, RIBLT_PROTOCOL_ID,
};

pub struct ReceiveNeighborSymbolsTask {
    identifier: Arc<dyn NodeIdentifier<NodeAddress, NodeAddress> + Send + Sync>,
    state: Arc<DefaultNodeState>,
    sending_states: Arc<RwLock<HashMap<NodeAddress, SendingState>>>,
    receiving_states: Arc<RwLock<HashMap<NodeAddress, ReceivingState>>>,
}

/// Per-session decode task. Owns the Decoder for one reconciliation session and
/// is the single consumer of its coded symbols, so decoding is serialised even
/// though inbound messages are handled in independent tasks. It pulls batches
/// off `rx`, peels them, stores newly decoded remote symbols, and replies with
/// either a credit/acknowledgement (RequestMoreSymbols carrying the running
/// received count) or, once decoding completes, a FinishedDecoding.
async fn run_receiver_session(
    state: Arc<DefaultNodeState>,
    own_id: NodeAddress,
    neighbor: NodeAddress,
    session_id: String,
    start_time: Instant,
    mut rx: mpsc::UnboundedReceiver<(u64, Vec<RIBLTCodedSymbol>)>,
) {
    let local = load_iblt_symbols(&state);
    let mut decoder = build_decoder_blocking(local).await;
    let mut stored_remote: usize = 0;

    // The decoder is positional: the k-th coded symbol fed to it must be encoder
    // index k. Batches arrive on independent connections and may be reordered,
    // so buffer them by start index and only feed the decoder the contiguous
    // run starting at `next_index`.
    let mut reorder: BTreeMap<u64, Vec<RIBLTCodedSymbol>> = BTreeMap::new();
    let mut next_index: u64 = 0;

    while let Some((start, symbols)) = rx.recv().await {
        reorder.insert(start, symbols);
        // Drain any other queued batches too before deciding what's contiguous.
        while let Ok((s, v)) = rx.try_recv() {
            reorder.insert(s, v);
        }

        // Pull off the contiguous prefix [next_index, ..) to feed in order.
        let mut batch = Vec::new();
        while let Some(chunk) = reorder.remove(&next_index) {
            next_index += chunk.len() as u64;
            batch.extend(chunk);
        }
        if batch.is_empty() {
            // Still waiting on the symbol at `next_index`; nothing to decode yet.
            continue;
        }
        let received_count = next_index as usize;

        let decode_start = Instant::now();
        let (next_decoder, peel) = process_batch_blocking(decoder, batch, stored_remote).await;
        decoder = next_decoder;
        stored_remote = peel.remote_total;
        histogram!("riblt_decode_duration_seconds", "neighbor" => format!("{:?}", neighbor))
            .record(decode_start.elapsed().as_secs_f64());

        store_symbols(&state, peel.remote_symbols).await;

        if peel.successful {
            info!("Peeling successful for neighbor {:?}", neighbor);
            let context = get_context();
            gauge!(
                "reconciliation_round_duration_seconds",
                "protocol" => "riblt",
                "neighbor" => format!("{:?}", neighbor),
                "run_id" => context.run_id().to_string(),
                "trial" => context.trial().to_string(),
                "similarity" => context.similarity().to_string()
            )
            .set(start_time.elapsed().as_secs_f64());
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
                format!("{:?}", own_id),
                format!("{:?}", neighbor),
                "riblt",
            );

            let _ = state
                .send_through_socket(
                    own_id.clone(),
                    Box::new(neighbor.clone()),
                    Box::new(RIBLTDecodedAllMessage::new(
                        RIBLTMessageType::new(RIBLTMessageTypeValues::FinishedDecoding),
                        Some(RIBLT_PROTOCOL_ID),
                        session_id.clone(),
                    )),
                )
                .await;
            break;
        }

        // Not done yet: acknowledge progress, which also grants the sender more
        // credit to keep streaming.
        let _ = state
            .send_through_socket(
                own_id.clone(),
                Box::new(neighbor.clone()),
                Box::new(RIBLTRequestMoreSymbolsMessage::new(
                    RIBLTMessageType::new(RIBLTMessageTypeValues::RequestMoreSymbols),
                    Some(RIBLT_PROTOCOL_ID),
                    session_id.clone(),
                    received_count as u64,
                )),
            )
            .await;
    }
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

    /// FinishedDecoding from the peer: the peer reconciled everything we were
    /// streaming, so tear down the sending session and wake the send loop so it
    /// observes the removal and stops.
    async fn receive_symbols_neighbor_decoded(&self, neighbor: NodeAddress, session_id: String) {
        info!("Neighbor successfully decoded symbols");

        let mut guard = self.sending_states.write().await;
        match guard.get(&neighbor) {
            Some(status) if status.session_id == session_id => {
                let notify = status.resend_notify.clone();
                guard.remove(&neighbor);
                notify.notify_one();
            }
            Some(_) => {
                info!(
                    "Session ID mismatch, ignoring FinishedDecoding for neighbor {:?}",
                    neighbor
                );
            }
            None => {}
        }
    }

    /// SendSymbol from the peer: route the coded symbols to the per-session
    /// decode task, creating that task (and its session state) on first sight of
    /// a session.
    async fn receive_incoming_symbols(
        &self,
        message: RIBLTSendSymbolMessage,
        neighbor: NodeAddress,
    ) {
        let msg_session_id = message.session_id().clone();

        // Atomically find-or-create the session and obtain the channel to feed.
        let tx = {
            let mut guard = self.receiving_states.write().await;
            match guard.get(&neighbor) {
                Some(status) if status.session_id == msg_session_id => status.symbol_tx.clone(),
                _ => {
                    // New session (or a session change): replacing the entry drops
                    // any previous sender, which ends the stale decode task.
                    info!("Starting receive session for neighbor {:?}", neighbor);
                    let (tx, rx) = mpsc::unbounded_channel();
                    let start_time = Instant::now();
                    guard.insert(
                        neighbor.clone(),
                        ReceivingState::new(msg_session_id.clone(), start_time, tx.clone()),
                    );

                    let state = self.state.clone();
                    let own_id = self.identifier.connection_info().clone();
                    let neighbor_clone = neighbor.clone();
                    let session_clone = msg_session_id.clone();
                    spawn!({
                        run_receiver_session(
                            state,
                            own_id,
                            neighbor_clone,
                            session_clone,
                            start_time,
                            rx,
                        )
                        .await;
                    });
                    tx
                }
            }
        };

        if tx
            .send((message.start_index(), message.symbols().clone()))
            .is_err()
        {
            // Decode task already finished for this session; nothing to do.
            info!("Receive session for {:?} already closed, dropping symbols", neighbor);
        }
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
                                    // Advance the acknowledged count (monotonically,
                                    // since acks may arrive out of order) and wake
                                    // the send loop to use the freed-up credit.
                                    let ack = msg.received_count() as usize;
                                    if ack > status.acked {
                                        status.acked = ack;
                                    }
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
