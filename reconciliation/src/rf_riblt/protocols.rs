use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use connection::{
    node::{
        default::{
            DefaultNodeSocket, DefaultNodeSocketTaskMetadata, PeriodicDefaultNodeSocketTask,
        },
        port::{ConnectionInfo, NodeAddress},
        NodeSocketTaskMetadata, PeriodicNodeSocketTask,
    },
    route::{default::NodeSocketRouteId, RouteHandler, RouteStorage, RouteTask},
};
use membership::{Membership, MembershipNeighbor, MembershipNeighbors};
use message::Message;
use protocol::{deserializer::ProtocolDeserializer, Protocol};
use crate::algorithms::ribbon::{Mode, Params, RibbonBuilder};
use runtime::time::{PeriodTimeUnit, TokioPeriodTimeUnit};
use state::node::{DefaultNodeState, NodeState};
use tokio::sync::{Mutex, RwLock};
use tracing::info;

use runtime::spawn;

use crate::{rf_riblt::receiver::ReceiveRfRibltMessageTask, ReconciliationProtocol};

use super::{
    messages::{RfRibltCodedSymbol, RfRibltHandshakeMessage, RfRibltSComSendSymbolMessage},
    RfRibltHasher, RfRibltProtocol, SComReconciliationState, SComSendingState,
    RF_FINGERPRINT_BITS, RF_OVERHEAD, RF_RIBLT_PROTOCOL_ID, RF_RIBBON_WIDTH, RIBLT_BATCH_SIZE,
};

impl RfRibltProtocol {
    fn shared_handle(&self) -> Arc<Self> {
        Arc::new(Self {
            state: self.state.clone(),
            port: self.port.clone(),
            deserializer: self.deserializer.clone(),
            filter_receiving_states: self.filter_receiving_states.clone(),
            scom_sending_states: self.scom_sending_states.clone(),
            scom_receiving_states: self.scom_receiving_states.clone(),
            pending_value_fetch_sessions: self.pending_value_fetch_sessions.clone(),
            last_reconciled_fingerprint: self.last_reconciled_fingerprint.clone(),
            reconciliation_initiated_with: self.reconciliation_initiated_with.clone(),
            round_start_times: self.round_start_times.clone(),
            captured_stn: self.captured_stn.clone(),
        })
    }

    /// Build a ribbon filter from the given keys and serialize it to bytes.
    /// Returns (filter_bytes, ribbon_seed) or None if construction fails.
    /// Format: [m: u64][w: u64][r: u64][seed: u64][mode: u8][packed_len: u64][packed: packed_len bytes]
    /// Each row is packed as ceil(r/8) bytes (only r meaningful bits per row, not stride_words*8).
    pub fn build_filter_bytes(keys: &[String]) -> Option<(Vec<u8>, u64)> {
        let ribbon_seed: u64 = rand::random();
        let n = keys.len().max(1);

        let params = Params::from_expected_items(
            n,
            RF_OVERHEAD,
            RF_RIBBON_WIDTH as usize,
            RF_FINGERPRINT_BITS as usize,
            Mode::Homogeneous,
        )
        .ok()?
        .with_seed(ribbon_seed);
        let builder = RibbonBuilder::new(params, RfRibltHasher::default()).ok()?;
        let filter = builder.build(keys).ok()?;

        let built_params = filter.params();
        let z_raw = filter.z_raw();
        let m = built_params.m;
        let r = built_params.r;
        let stride_words = r.div_ceil(64);
        let bytes_per_row = r.div_ceil(8);
        let packed_len = m * bytes_per_row;

        let mut bytes = Vec::with_capacity(8 * 4 + 1 + 8 + packed_len);
        bytes.extend_from_slice(&(m as u64).to_le_bytes());
        bytes.extend_from_slice(&(built_params.w as u64).to_le_bytes());
        bytes.extend_from_slice(&(r as u64).to_le_bytes());
        bytes.extend_from_slice(&built_params.seed.to_le_bytes());
        bytes.push(match built_params.mode {
            Mode::Standard => 0u8,
            Mode::Homogeneous => 1u8,
        });
        bytes.extend_from_slice(&(packed_len as u64).to_le_bytes());

        for row in 0..m {
            let base = row * stride_words;
            let mut bits_left = r;
            let mut word_i = 0;
            while bits_left > 0 {
                let take = bits_left.min(64);
                let take_bytes = take.div_ceil(8);
                let word = z_raw[base + word_i];
                let mask = if take == 64 { u64::MAX } else { (1u64 << take) - 1 };
                bytes.extend_from_slice(&(word & mask).to_le_bytes()[..take_bytes]);
                bits_left -= take;
                word_i += 1;
            }
        }

        Some((bytes, ribbon_seed))
    }

    /// Send the ribbon filter to `neighbor` as a single Handshake message.
    pub async fn send_filter_to_neighbor(
        state: Arc<DefaultNodeState>,
        neighbor: NodeAddress,
        session_id: String,
        filter_bytes: Vec<u8>,
        ribbon_seed: u64,
    ) {
        let _ = state
            .send_through_socket(
                state.node_identifier().connection_info().clone(),
                Box::new(neighbor),
                Box::new(RfRibltHandshakeMessage::new(
                    Some(RF_RIBLT_PROTOCOL_ID),
                    session_id,
                    ribbon_seed,
                    filter_bytes,
                )),
            )
            .await;
    }

    pub async fn stream_scom_symbols_to_neighbor(
        state: Arc<DefaultNodeState>,
        scom_sending_states: Arc<RwLock<HashMap<NodeAddress, SComSendingState>>>,
        neighbor: NodeAddress,
    ) {
        let mut current_index = 0;

        loop {
            let sending_state = {
                let states = scom_sending_states.read().await;
                match states.get(&neighbor) {
                    Some(s) => s.state.clone(),
                    None => break,
                }
            };

            if sending_state == SComReconciliationState::AwaitingConfirmation {
                tokio::task::yield_now().await;
                continue;
            }

            let (symbols, session_id) = {
                let mut states = scom_sending_states.write().await;
                let s = match states.get_mut(&neighbor) {
                    Some(s) => s,
                    None => break,
                };

                let mut symbols = Vec::new();
                for _ in 0..RIBLT_BATCH_SIZE {
                    let cs = s.local_iblt.get_coded_symbol(current_index);
                    symbols.push(RfRibltCodedSymbol {
                        sum: cs.sum,
                        hash: cs.hash,
                        count: cs.count,
                    });
                    current_index += 1;
                }

                s.state = SComReconciliationState::AwaitingConfirmation;
                (symbols, s.session_id.clone())
            };

            let _ = state
                .send_through_socket(
                    state.node_identifier().connection_info().clone(),
                    Box::new(neighbor.clone()),
                    Box::new(RfRibltSComSendSymbolMessage::new(
                        Some(RF_RIBLT_PROTOCOL_ID),
                        symbols,
                        session_id,
                    )),
                )
                .await;

            tokio::task::yield_now().await;
        }
    }

    async fn start_sending_filter(&self) -> Result<(), String> {
        info!("Starting RF-RIBLT: building ribbon filters for neighbors");

        let local_addr = self.state.node_identifier().connection_info();

        let connection_targets = self
            .state
            .membership()
            .read()
            .await
            .valid_connection_targets();

        for neighbor in connection_targets {
            if (local_addr.host(), local_addr.port()) >= (neighbor.host(), neighbor.port()) {
                continue;
            }

            if self.is_session_active(&neighbor).await {
                continue;
            }

            let storage = match self.state.get_storage("default".to_string()) {
                Some(s) => s,
                None => continue,
            };

            let keys: Vec<String> = storage
                .items()
                .into_iter()
                .map(|item| item.key().to_string())
                .collect();

            let (filter_bytes, ribbon_seed) = match Self::build_filter_bytes(&keys) {
                Some(result) => result,
                None => {
                    tracing::error!("Failed to build ribbon filter for neighbor {:?}", neighbor);
                    continue;
                }
            };

            let session_id = uuid::Uuid::new_v4().to_string();

            self.reconciliation_initiated_with
                .write()
                .await
                .insert(neighbor.clone());

            let state = self.state.clone();

            info!("Sending ribbon filter to neighbor {:?}", neighbor);

            spawn!({
                RfRibltProtocol::send_filter_to_neighbor(
                    state,
                    neighbor,
                    session_id,
                    filter_bytes,
                    ribbon_seed,
                )
                .await;
            });
        }

        Ok(())
    }
}

#[async_trait]
impl<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
    Protocol<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage> for RfRibltProtocol
where
    S: NodeState,
    T: RouteTask,
    M: NodeSocketTaskMetadata,
    R: MembershipNeighbors<MN>,
    N: Membership<R, MN>,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV>,
    CV: Sized,
    PTU: PeriodTimeUnit + Send + Sync,
    PT: PeriodicNodeSocketTask<PTU>,
    RHandler: RouteHandler + Send + Sync,
    RStorage: RouteStorage,
{
    fn deserializer(&self) -> Arc<dyn ProtocolDeserializer> {
        self.deserializer.clone()
    }

    fn deserialize_message(&self, bytes: Vec<u8>) -> Arc<dyn Message> {
        self.deserializer.deserialize(bytes)
    }

    fn id(&self) -> u64 {
        RF_RIBLT_PROTOCOL_ID
    }

    async fn init(&mut self) {
        let protocol_handle = self.shared_handle();

        self.state
            .add_socket_task_and_create(
                NodeSocketRouteId::new(self.port.clone(), RF_RIBLT_PROTOCOL_ID),
                Arc::new(ReceiveRfRibltMessageTask::new(protocol_handle.clone())),
                Box::new(move |port: NodeAddress| {
                    Arc::new(Mutex::new(DefaultNodeSocket::new(port)))
                }),
            )
            .unwrap();

        self.state
            .add_periodic_socket_task(
                self.port.clone(),
                Arc::new(PeriodicDefaultNodeSocketTask::new(
                    Arc::new(DefaultNodeSocketTaskMetadata::new(String::new())),
                    Arc::new(move || {
                        let protocol = protocol_handle.clone();
                        Box::pin(async move {
                            protocol.start_sending_filter().await?;
                            Ok(())
                        })
                    }),
                    Arc::new(TokioPeriodTimeUnit::new(std::time::Duration::from_secs(
                        3600,
                    ))),
                )),
            )
            .await
            .unwrap();
    }
}

#[async_trait]
impl<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
    ReconciliationProtocol<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
    for RfRibltProtocol
where
    S: NodeState,
    T: RouteTask,
    M: NodeSocketTaskMetadata,
    R: MembershipNeighbors<MN>,
    N: Membership<R, MN>,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV>,
    CV: Sized,
    PTU: PeriodTimeUnit + Send + Sync,
    PT: PeriodicNodeSocketTask<PTU>,
    RHandler: RouteHandler + Send + Sync,
    RStorage: RouteStorage,
{
    fn state(&self) {
        info!("RF-RIBLT protocol state initialized");
    }
}
