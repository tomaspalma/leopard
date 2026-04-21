use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    hash::{Hash, Hasher},
    sync::Arc,
    time::Instant,
};

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
use state::node::NodeState;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tracing::info;

use crate::{rbf_riblt::receiver::ReceiveRbfRibltMessageTask, ReconciliationProtocol};

use super::{
    messages::{
        RbfRibltBloomFilterSliceMessage, RbfRibltCodedSymbol, RbfRibltHandshakeMessage,
        RbfRibltMessageType, RbfRibltMessageTypeValues, RbfRibltSComSendSymbolMessage,
    },
    BloomSendingState, RatelessBF, RbfRibltProtocol, SComReconciliationState, SComSendingState,
    BLOOM_HASHES, RBF_RIBLT_PROTOCOL_ID, RIBLT_BATCH_SIZE,
};
use crate::riblt::messages::RIBLTSymbol;
use riblt::RatelessIBLT;

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
        })
    }

    fn compute_local_fingerprint(
        state: &Arc<state::node::DefaultNodeState>,
    ) -> Option<u64> {
        let storage = state.get_storage("default".to_string())?;
        let mut pairs: Vec<(String, String)> = storage
            .items()
            .into_iter()
            .map(|item| (item.key().to_string(), item.value().to_string()))
            .collect();
        pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

        let mut hasher = DefaultHasher::new();
        pairs.len().hash(&mut hasher);
        for (k, v) in pairs {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }

        Some(hasher.finish())
    }

    fn build_scom_iblt(elements: &[String]) -> RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>> {
        let mut symbols = HashSet::new();
        for key in elements {
            symbols.insert(RIBLTSymbol {
                key: key.clone(),
                value: String::new(),
            });
        }
        RatelessIBLT::new(symbols)
    }

    pub async fn start_scom_reconciliation_with_neighbor(
        protocol: Arc<Self>,
        neighbor: NodeAddress,
        session_id: String,
        s_com: Vec<String>,
    ) {
        if protocol.scom_sending_states.read().await.contains_key(&neighbor) {
            return;
        }

        let local_iblt = Self::build_scom_iblt(&s_com);
        protocol.scom_sending_states.write().await.insert(
            neighbor.clone(),
            SComSendingState {
                state: SComReconciliationState::SendingSymbols,
                local_iblt,
                session_id,
                start_time: Instant::now(),
            },
        );

        let protocol_clone = protocol.clone();
        runtime::spawn!({
            Self::scom_sending_symbols_sequence(protocol_clone, neighbor).await;
        });
    }

    async fn scom_sending_symbols_sequence(protocol: Arc<Self>, neighbor: NodeAddress) {
        let mut current_index = 0;
        let mut wait_time_ms = 0;

        while protocol.scom_sending_states.read().await.contains_key(&neighbor) {
            let state = protocol
                .scom_sending_states
                .read()
                .await
                .get(&neighbor)
                .map(|s| s.state.clone());

            if state == Some(SComReconciliationState::AwaitingConfirmation) {
                sleep(Duration::from_millis(100)).await;
                wait_time_ms += 100;
                if wait_time_ms >= 5000 {
                    if let Some(status) = protocol.scom_sending_states.write().await.get_mut(&neighbor)
                    {
                        status.state = SComReconciliationState::SendingSymbols;
                    }
                    wait_time_ms = 0;
                }
                continue;
            }

            wait_time_ms = 0;
            let mut symbols = Vec::new();
            let session_id;

            {
                let mut states = protocol.scom_sending_states.write().await;
                let Some(status) = states.get_mut(&neighbor) else {
                    break;
                };
                session_id = status.session_id.clone();

                for _ in 0..RIBLT_BATCH_SIZE {
                    let coded_symbol = status.local_iblt.get_coded_symbol(current_index);
                    symbols.push(RbfRibltCodedSymbol {
                        sum: coded_symbol.sum,
                        hash: coded_symbol.hash,
                        count: coded_symbol.count,
                    });
                    current_index += 1;
                }

                status.state = SComReconciliationState::AwaitingConfirmation;
            }

            let msg = RbfRibltSComSendSymbolMessage::new(
                Some(RBF_RIBLT_PROTOCOL_ID),
                symbols,
                session_id,
            );

            let _ = protocol
                .state
                .send_through_socket(
                    protocol.state.node_identifier().connection_info().clone(),
                    Box::new(neighbor.clone()),
                    Box::new(msg),
                )
                .await;
        }
    }

    async fn periodic_bloom_exchange(protocol: Arc<Self>) -> Result<(), String> {
        let storage = match protocol.state.get_storage("default".to_string()) {
            Some(storage) => storage,
            None => return Ok(()),
        };

        let local_fingerprint = match Self::compute_local_fingerprint(&protocol.state) {
            Some(f) => f,
            None => return Ok(()),
        };

        let local_keys: Vec<String> = storage
            .items()
            .into_iter()
            .map(|item| item.key().to_string())
            .collect();

        let m_bits = ((local_keys.len().max(1)) as f64 / std::f64::consts::LN_2).ceil() as usize;
        let own = protocol.state.node_identifier().connection_info().clone();
        let targets = protocol
            .state
            .membership()
            .read()
            .await
            .valid_connection_targets();

        for neighbor in targets {
            if protocol.scom_sending_states.read().await.contains_key(&neighbor)
                || protocol.scom_receiving_states.read().await.contains_key(&neighbor)
                || protocol
                    .pending_value_fetch_sessions
                    .read()
                    .await
                    .contains_key(&neighbor)
            {
                continue;
            }

            if protocol
                .bloom_receiving_states
                .read()
                .await
                .get(&neighbor)
                .map(|s| s.riblt_started)
                .unwrap_or(false)
            {
                continue;
            }

            if protocol
                .last_reconciled_fingerprint
                .read()
                .await
                .get(&neighbor)
                .copied()
                == Some(local_fingerprint)
            {
                continue;
            }

            let (session_id, slice_index, state_m_bits, should_send_handshake) = {
                let mut states = protocol.bloom_sending_states.write().await;
                let entry = states.entry(neighbor.clone()).or_insert_with(|| {
                    BloomSendingState::new(uuid::Uuid::new_v4().to_string(), m_bits)
                });

                let slice_idx = entry.next_slice_index;
                entry.next_slice_index += 1;

                (
                    entry.session_id.clone(),
                    slice_idx,
                    entry.m_bits,
                    slice_idx == 0,
                )
            };

            if should_send_handshake {
                let handshake = RbfRibltHandshakeMessage::new(
                    RbfRibltMessageType::new(RbfRibltMessageTypeValues::Handshake),
                    Some(RBF_RIBLT_PROTOCOL_ID),
                    session_id.clone(),
                );

                let _ = protocol
                    .state
                    .send_through_socket(
                        own.clone(),
                        Box::new(neighbor.clone()),
                        Box::new(handshake),
                    )
                    .await;
            }

            let seeds = [rand::random::<u64>(), rand::random::<u64>()];
            let mut rateless_bf = RatelessBF::new(local_keys.clone(), state_m_bits);
            rateless_bf.extend_with_seeds(seeds);

            if let Some(filter) = rateless_bf.latest_filter() {
                let msg = RbfRibltBloomFilterSliceMessage::new(
                    Some(RBF_RIBLT_PROTOCOL_ID),
                    session_id,
                    slice_index,
                    filter.bit_len(),
                    BLOOM_HASHES,
                    filter.seeds(),
                    filter.to_bytes(),
                );

                let _ = protocol
                    .state
                    .send_through_socket(own.clone(), Box::new(neighbor), Box::new(msg))
                    .await;
            }
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
                            RbfRibltProtocol::periodic_bloom_exchange(protocol).await?;
                            Ok(())
                        })
                    }),
                    Arc::new(TokioPeriodTimeUnit::new(std::time::Duration::from_secs(5))),
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
