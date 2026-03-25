pub mod messages;

use runtime::spawn;

use tracing::error;

use dashmap::DashMap;

use message::Message;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use async_trait::async_trait;
use protocol::{deserializer::ProtocolDeserializer, Protocol};
use state::node::{DefaultNodeState, NodeState};

use connection::{
    node::{
        default::{
            DefaultNodeSocket, DefaultNodeSocketTaskMetadata, PeriodicDefaultNodeSocketTask,
        },
        port::{ConnectionInfo, NodeAddress},
        NodeSocketTaskMetadata, PeriodicNodeSocketTask,
    },
    request::handler::default::{TestMessage, TestMessageType},
    route::{default::NodeSocketRouteId, RouteHandler, RouteStorage, RouteTask},
};
use membership::{Membership, MembershipNeighbor, MembershipNeighbors};
use runtime::time::{PeriodTimeUnit, TokioPeriodTimeUnit};

use crate::{
    riblt::messages::{RIBLTCodedSymbol, RIBLTMessageType, RIBLTSendSymbolMessage, RIBLTSymbol},
    ReconciliationProtocol,
};
use riblt::RatelessIBLT;
use rkyv::{from_bytes, rancor::Error};
use state::storage::StorageAction;
use std::collections::HashSet;
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone, PartialEq)]
pub enum ReconciliationState {
    SendingSymbols,
    AwaitingConfirmation,
}

const RIBLT_PROTOCOL_ID: u64 = 1;
const BATCH_SIZE: usize = 5;
const BATCH_INTERVAL: Duration = Duration::from_millis(5000);

pub struct RIBLT {
    id: u64,
    state: Arc<DefaultNodeState>,
    port: NodeAddress,
    deserializer: Arc<RIBLTDeserializer>,
    reconciliation_states: Arc<DashMap<NodeAddress, ReconciliationState>>,
    iblt: Arc<tokio::sync::RwLock<RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>>>>,
}

impl RIBLT {
    pub fn new(state: Arc<DefaultNodeState>, port: NodeAddress) -> Self {
        Self {
            id: RIBLT_PROTOCOL_ID,
            state,
            port,
            deserializer: Arc::new(RIBLTDeserializer::default()),
            reconciliation_states: Arc::new(DashMap::new()),
            iblt: Arc::new(tokio::sync::RwLock::new(RatelessIBLT::new(HashSet::new()))),
        }
    }

    async fn sending_symbols_sequence(
        state: Arc<DefaultNodeState>,
        own_address: NodeAddress,
        neighbor_address: NodeAddress,
        protocol_id: u64,
        reconciliation_states: Arc<DashMap<NodeAddress, ReconciliationState>>,
        iblt: Arc<tokio::sync::RwLock<RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>>>>,
    ) {
        info!(
            "Running sending symbols sequence from {:?} to {:?}",
            own_address, neighbor_address
        );

        let mut current_index = 0;

        while reconciliation_states.contains_key(&neighbor_address) {
            let mut symbols = Vec::new();

            {
                let mut iblt_guard = iblt.write().await;
                for _ in 0..BATCH_SIZE {
                    let coded_symbol = iblt_guard.get_coded_symbol(current_index);

                    let symbol_message = RIBLTCodedSymbol {
                        sum: coded_symbol.sum,
                        hash: coded_symbol.hash,
                        count: coded_symbol.count,
                    };
                    symbols.push(symbol_message);

                    current_index += 1;
                }
            }

            state
                .send_through_socket(
                    own_address.clone(),
                    Box::new(neighbor_address.clone()),
                    Box::new(RIBLTSendSymbolMessage::new(
                        RIBLTMessageType::new(),
                        Some(protocol_id),
                        symbols,
                    )),
                )
                .await
                .unwrap();

            info!(
                "Sent batch of {} symbols up to index {}",
                BATCH_SIZE, current_index
            );

            reconciliation_states
                .get_mut(&neighbor_address)
                .map(|mut state| {
                    *state = ReconciliationState::AwaitingConfirmation;
                });

            sleep(BATCH_INTERVAL).await;
        }
    }

    async fn reconciliation_mechanism(
        state: Arc<DefaultNodeState>,
        port: NodeAddress,
        protocol_id: u64,
        reconciliation_states: Arc<DashMap<NodeAddress, ReconciliationState>>,
        iblt: Arc<tokio::sync::RwLock<RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>>>>,
    ) -> Result<(), String> {
        info!("Ran reconciliation mechanism");

        let connection_targets = {
            let membership_arc = state.membership();
            let membership_guard = membership_arc.read().unwrap();

            let neighbors_arc = membership_guard.representation().neighbors();
            let neighbors_guard = neighbors_arc.read().unwrap();

            neighbors_guard
                .iter()
                .map(|n| n.read().unwrap())
                .filter(|n| !n.tainted())
                .map(|n| n.identifier().connection_info())
                .collect::<Vec<_>>()
        };

        for info in connection_targets {
            if let Some(_) = reconciliation_states.get(&info) {
                continue;
            }

            reconciliation_states.insert(info.clone(), ReconciliationState::SendingSymbols);

            let state_clone = state.clone();
            let port_clone = port.clone();
            let info_clone = info.clone();
            let protocol_id_clone = protocol_id;
            let reconciliation_states_clone = reconciliation_states.clone();
            let iblt_clone = iblt.clone();

            spawn!({
                RIBLT::sending_symbols_sequence(
                    state_clone,
                    port_clone,
                    info_clone,
                    protocol_id_clone,
                    reconciliation_states_clone,
                    iblt_clone,
                )
                .await;
            });
        }

        Ok(())
    }
}

pub struct RibltTask {}

impl RibltTask {
    pub fn new() -> Self {
        Self {}
    }
}

impl RouteTask for RibltTask {
    fn run(&self, message: Vec<u8>) {
        let deserialized_message = RIBLTDeserializer::new().deserialize(message);

        let message = deserialized_message
            .as_any()
            .downcast_ref::<RIBLTSendSymbolMessage>();

        match message {
            Some(msg) => {
                info!("Received RIBLT message: {:?}", msg);
            }
            None => error!("Failed to downcast message to RIBLTSendSymbolMessage"),
        }

        // 2. We have to start decoding the symbols
        //
        // 3. If we cannot decode all symbols, we have to inform the sender
    }
}

#[derive(Default)]
pub struct RIBLTDeserializer {}

impl RIBLTDeserializer {
    pub fn new() -> Self {
        Self {}
    }
}

impl ProtocolDeserializer for RIBLTDeserializer {
    fn deserialize(&self, bytes: Vec<u8>) -> Arc<dyn Message> {
        if bytes.len() < 8 {
            return Arc::new(TestMessage::new(Arc::new(TestMessageType::new()), None));
        }

        let payload = &bytes[8..];

        match from_bytes::<RIBLTSendSymbolMessage, Error>(payload) {
            Ok(msg) => Arc::new(msg),
            Err(e) => {
                error!("Failed to deserialize RIBLT message: {}", e);
                Arc::new(TestMessage::new(Arc::new(TestMessageType::new()), None))
            }
        }
    }
}

impl riblt::Symbol for RIBLTSymbol {
    const BYTE_ARRAY_LENGTH: usize = 128;

    fn encode_to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.key.as_bytes().to_vec();
        bytes.extend_from_slice(&self.value);
        if bytes.len() < Self::BYTE_ARRAY_LENGTH {
            bytes.resize(Self::BYTE_ARRAY_LENGTH, 0);
        }
        bytes
    }

    fn decode_from_bytes(bytes: &Vec<u8>) -> Self {
        let key_end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        let key = String::from_utf8_lossy(&bytes[..key_end]).to_string();
        let value = bytes[key_end..].to_vec();
        Self { key, value }
    }
}

#[async_trait]
impl<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
    Protocol<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage> for RIBLT
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
        Arc::new(RIBLTDeserializer::default())
    }

    fn deserialize_message(&self, bytes: Vec<u8>) -> Arc<dyn Message> {
        self.deserializer.deserialize(bytes)
    }

    fn id(&self) -> u64 {
        self.id
    }

    async fn init(&mut self) {
        let state_handle = self.state.clone();
        let port_for_closure = self.port.clone();
        let protocol_id = self.id;
        let reconciliation_states = self.reconciliation_states.clone();
        let iblt_handle = self.iblt.clone();

        if let Some(storage) = self.state.get_storage("default".to_string()) {
            let items = storage.items();
            let mut symbols = HashSet::new();

            for item in items {
                symbols.insert(RIBLTSymbol {
                    key: item.key().to_string(),
                    value: item.value().as_bytes().to_vec(),
                });
            }

            *self.iblt.write().await = RatelessIBLT::new(symbols);

            let iblt_handle = iblt_handle.clone();
            let storage_clone = storage.clone();
            storage.add_listener(
                StorageAction::Insert,
                Box::new(move |_item| {
                    let iblt = iblt_handle.clone();
                    let storage = storage_clone.clone();

                    spawn!({
                        let items = storage.items();
                        let mut symbols = HashSet::new();

                        for item in items {
                            symbols.insert(RIBLTSymbol {
                                key: item.key().to_string(),
                                value: item.value().as_bytes().to_vec(),
                            });
                        }

                        let mut guard = iblt.write().await;
                        *guard = RatelessIBLT::new(symbols);
                    });
                }),
            );
        } else {
            info!("No default storage found");
        }

        self.state
            .add_socket_task_and_create(
                NodeSocketRouteId::new(self.port.clone(), protocol_id),
                Arc::new(RibltTask::new()),
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
                        let state = state_handle.clone();
                        let port = port_for_closure.clone();
                        let protocol_id = protocol_id;
                        let reconciliation_states = reconciliation_states.clone();
                        let iblt = iblt_handle.clone();

                        Box::pin(async move {
                            Self::reconciliation_mechanism(
                                state,
                                port,
                                protocol_id,
                                reconciliation_states,
                                iblt,
                            )
                            .await
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
    ReconciliationProtocol<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage> for RIBLT
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
        info!("Reconciliation States:");
        for r in self.reconciliation_states.iter() {
            info!("  {:?}: {:?}", r.key(), r.value());
        }
    }
}
