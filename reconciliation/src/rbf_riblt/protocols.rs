use std::{collections::HashMap, sync::Arc, time::Instant};

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
use tokio::time::{timeout, Duration};
use tracing::info;

// Bloom-phase flow control. The sender keeps at most BLOOM_SEND_WINDOW slices in
// flight (sent but unacknowledged); with a window of 1 this is stop-and-wait,
// which bounds the receiver's per-session backlog to a single in-memory slice.
const BLOOM_SEND_WINDOW: u64 = 1;
// Safety net so a dropped ack can't wedge the sender forever; on expiry it
// re-checks the session state and either resumes or observes the stop.
const BLOOM_ACK_TIMEOUT: Duration = Duration::from_millis(5000);

use runtime::spawn;

use crate::{
    rbf_riblt::receiver::ReceiveRbfRibltMessageTask,
    riblt::{receiver::ReceiveNeighborSymbolsTask, RIBLT_PROTOCOL_ID},
    ReconciliationProtocol,
};

use crate::algorithms::rbf::bloom::BloomFilter;
use super::{
    messages::RbfRibltBloomFilterSliceMessage,
    BloomSendingState, RbfRibltProtocol, BLOOM_HASHES, RBF_RIBLT_PROTOCOL_ID,
};

impl RbfRibltProtocol {
    fn shared_handle(&self) -> Arc<Self> {
        Arc::new(Self {
            state: self.state.clone(),
            port: self.port.clone(),
            deserializer: self.deserializer.clone(),
            bloom_sending_states: self.bloom_sending_states.clone(),
            bloom_receiving_states: self.bloom_receiving_states.clone(),
            scom_engine: self.scom_engine.clone(),
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

    pub async fn stream_fixed_slices_to_neighbor(
        state: Arc<DefaultNodeState>,
        bloom_sending_states: Arc<RwLock<HashMap<NodeAddress, BloomSendingState>>>,
        neighbor: NodeAddress,
        keys: Vec<String>,
    ) {
        loop {
            // Park while the in-flight window is full so the sender stays in
            // lockstep with the receiver's slice processing instead of flooding
            // it. An ack (or the stop signal) wakes us via ack_notify.
            let (in_flight, ack_notify) = {
                let sending = bloom_sending_states.read().await;
                match sending.get(&neighbor) {
                    Some(s) => (s.next_slice_index.saturating_sub(s.acked), s.ack_notify.clone()),
                    // Session removed (stop signal received) -> stop.
                    None => break,
                }
            };

            if in_flight >= BLOOM_SEND_WINDOW {
                let _ = timeout(BLOOM_ACK_TIMEOUT, ack_notify.notified()).await;
                continue;
            }

            if !Self::send_next_bloom_slice(&state, &bloom_sending_states, &neighbor, &keys).await {
                break;
            }
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

            let keys: Vec<String> = storage.keys();

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
            // Stamp the reconciliation start here, at the very beginning of the
            // bloom phase, so the round-duration metric covers the whole
            // reconciliation (bloom + scom) rather than just the scom phase.
            self.round_start_times
                .write()
                .await
                .insert(neighbor.clone(), Instant::now());

            let state = self.state.clone();
            let bloom_sending_states = self.bloom_sending_states.clone();

            info!("Spawning stream slices to neighbor");

            // The initiator's key set is stable for the duration of a round, so
            // snapshot it once (above) and reuse the fixed-slice streamer rather
            // than re-cloning the whole store on every slice.
            spawn!({
                RbfRibltProtocol::stream_fixed_slices_to_neighbor(
                    state,
                    bloom_sending_states,
                    neighbor,
                    keys,
                )
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

        // The scom streaming phase reuses the standalone RIBLT engine route: scom
        // symbol/credit batches are tagged with RIBLT_PROTOCOL_ID and decoded by
        // the same shared ReceiveNeighborSymbolsTask, here wired to this protocol's
        // scom engine. The socket already exists from the registration above, so
        // this only adds a second route under a different protocol id on the same
        // port.
        self.state
            .add_socket_task_and_create(
                NodeSocketRouteId::new(self.port.clone(), RIBLT_PROTOCOL_ID),
                Arc::new(ReceiveNeighborSymbolsTask::new(self.scom_engine.clone())),
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
