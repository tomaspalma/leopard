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
use runtime::time::{PeriodTimeUnit, TokioPeriodTimeUnit};
use state::node::{DefaultNodeState, NodeState};
use tokio::sync::{Mutex, RwLock};
use tracing::info;

use runtime::spawn;

use crate::{rbf_riblt::receiver::ReceiveRbfRibltMessageTask, ReconciliationProtocol};

use super::{
    bloom::BloomFilter,
    messages::{
        RbfRibltBloomFilterSliceMessage, RbfRibltCodedSymbol, RbfRibltSComSendSymbolMessage,
    },
    BloomSendingState, RbfRibltProtocol, SComReconciliationState, SComSendingState, BLOOM_HASHES,
    RBF_RIBLT_PROTOCOL_ID, RIBLT_BATCH_SIZE,
};

impl RbfRibltProtocol {
    fn shared_handle(&self) -> Arc<Self> {
        Arc::new(Self {
            state: self.state.clone(),
            port: self.port.clone(),
            deserializer: self.deserializer.clone(),
            bloom_sending_states: self.bloom_sending_states.clone(),
            bloom_receiving_states: self.bloom_receiving_states.clone(),
            scom_sending_states: self.scom_sending_states.clone(),
            scom_receiving_states: self.scom_receiving_states.clone(),
            pending_value_fetch_sessions: self.pending_value_fetch_sessions.clone(),
            last_reconciled_fingerprint: self.last_reconciled_fingerprint.clone(),
            reconciliation_initiated_with: self.reconciliation_initiated_with.clone(),
            round_start_times: self.round_start_times.clone(),
            captured_stn: self.captured_stn.clone(),
        })
    }

    async fn send_next_bloom_slice(
        state: &Arc<DefaultNodeState>,
        bloom_sending_states: &Arc<RwLock<HashMap<NodeAddress, BloomSendingState>>>,
        neighbor: &NodeAddress,
        keys: &[String],
    ) -> bool {
        let (session_id, slice_index, m_bits) = {
            let sending = bloom_sending_states.read().await;
            match sending.get(neighbor) {
                Some(s) => (s.session_id.clone(), s.next_slice_index, s.m_bits),
                None => return false,
            }
        };

        let seeds = [slice_index, slice_index.wrapping_add(1)];
        let mut filter =
            BloomFilter::<String>::from_raw_parts_with_seeds(m_bits, BLOOM_HASHES, seeds);
        for key in keys {
            filter.insert(key);
        }

        let slice = filter.bitslice();
        let bits: Vec<u8> = (0..slice.len())
            .step_by(8)
            .map(|start| {
                let end = (start + 8).min(slice.len());
                (start..end).fold(0u8, |byte, i| {
                    if slice[i] {
                        byte | (1 << (i - start))
                    } else {
                        byte
                    }
                })
            })
            .collect();

        let _ = state
            .send_through_socket(
                state.node_identifier().connection_info().clone(),
                Box::new(neighbor.clone()),
                Box::new(RbfRibltBloomFilterSliceMessage::new(
                    Some(RBF_RIBLT_PROTOCOL_ID),
                    session_id,
                    slice_index,
                    m_bits,
                    BLOOM_HASHES,
                    seeds,
                    bits,
                )),
            )
            .await;

        if let Some(s) = bloom_sending_states.write().await.get_mut(neighbor) {
            s.next_slice_index += 1;
        }

        true
    }

    async fn stream_slices_to_neighbor(
        state: Arc<DefaultNodeState>,
        bloom_sending_states: Arc<RwLock<HashMap<NodeAddress, BloomSendingState>>>,
        neighbor: NodeAddress,
    ) {
        loop {
            let storage = match state.get_storage("default".to_string()) {
                Some(s) => s,
                None => break,
            };

            let keys: Vec<String> = storage
                .items()
                .into_iter()
                .map(|item| item.key().to_string())
                .collect();

            if !Self::send_next_bloom_slice(&state, &bloom_sending_states, &neighbor, &keys).await {
                break;
            }

            tokio::task::yield_now().await;
        }
    }

    pub async fn stream_fixed_slices_to_neighbor(
        state: Arc<DefaultNodeState>,
        bloom_sending_states: Arc<RwLock<HashMap<NodeAddress, BloomSendingState>>>,
        neighbor: NodeAddress,
        keys: Vec<String>,
    ) {
        loop {
            if !Self::send_next_bloom_slice(&state, &bloom_sending_states, &neighbor, &keys).await {
                break;
            }

            tokio::task::yield_now().await;
        }
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
                    symbols.push(RbfRibltCodedSymbol {
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
                    Box::new(RbfRibltSComSendSymbolMessage::new(
                        Some(RBF_RIBLT_PROTOCOL_ID),
                        symbols,
                        session_id,
                    )),
                )
                .await;

            tokio::task::yield_now().await;
        }
    }

    async fn start_sending_slices(&self) -> Result<(), String> {
        info!("Starting the process of sending RBF-RIBLT slices to neighbors");

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

            let already_active = self.is_session_active(&neighbor).await;
            if already_active {
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

            let m_bits = ((keys.len().max(1)) as f64 / std::f64::consts::LN_2).ceil() as usize;
            let session_id = uuid::Uuid::new_v4().to_string();
            self.bloom_sending_states
                .write()
                .await
                .insert(neighbor.clone(), BloomSendingState::new(session_id, m_bits));
            self.reconciliation_initiated_with
                .write()
                .await
                .insert(neighbor.clone());

            let state = self.state.clone();
            let bloom_sending_states = self.bloom_sending_states.clone();

            info!("Spawning stream slices to neighbor");

            spawn!({
                RbfRibltProtocol::stream_slices_to_neighbor(state, bloom_sending_states, neighbor)
                    .await;
            });
        }

        Ok(())
    }
}

#[async_trait]
impl<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
    Protocol<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage> for RbfRibltProtocol
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
        RBF_RIBLT_PROTOCOL_ID
    }

    async fn init(&mut self) {
        let protocol_handle = self.shared_handle();

        self.state
            .add_socket_task_and_create(
                NodeSocketRouteId::new(self.port.clone(), RBF_RIBLT_PROTOCOL_ID),
                Arc::new(ReceiveRbfRibltMessageTask::new(protocol_handle.clone())),
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
                            protocol.start_sending_slices().await?;
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
    for RbfRibltProtocol
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
        info!("RBF-RIBLT protocol state initialized");
    }
}
