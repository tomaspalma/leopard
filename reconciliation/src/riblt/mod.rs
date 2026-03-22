use message::{Message, MessageType, MessageTypeValues};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use async_trait::async_trait;
use protocol::{deserializer::ProtocolDeserializer, Protocol, ProtocolIDGenerator};
use state::node::{DefaultNodeState, NodeState};

use connection::{
    node::{
        default::{
            DefaultNodeSocket, DefaultNodeSocketTask, DefaultNodeSocketTaskMetadata,
            PeriodicDefaultNodeSocketTask,
        },
        port::{ConnectionInfo, NodeAddress},
        NodeSocketTaskMetadata, PeriodicNodeSocketTask,
    },
    request::handler::default::{TestMessage, TestMessageType},
    route::{default::NodeSocketRouteId, RouteHandler, RouteStorage, RouteTask},
};
use membership::{Membership, MembershipNeighbor, MembershipNeighbors};
use runtime::{
    time::{PeriodTimeUnit, TokioPeriodTimeUnit},
    RUNTIME,
};

use crate::ReconciliationProtocol;

const RIBLT_PROTOCOL_ID: u64 = 1;

pub struct RIBLT {
    id: u64,
    state: Arc<DefaultNodeState>,
    port: NodeAddress,
    deserializer: Arc<RIBLTDeserializer>,
}

impl RIBLT {
    pub fn new(state: Arc<DefaultNodeState>, port: NodeAddress) -> Self {
        Self {
            id: RIBLT_PROTOCOL_ID,
            state,
            port,
            deserializer: Arc::new(RIBLTDeserializer::default()),
        }
    }

    async fn sending_symbols_sequence(
        state: Arc<DefaultNodeState>,
        own_address: NodeAddress,
        neighbor_address: NodeAddress,
        protocol_id: u64,
    ) {
        info!(
            "Running sending symbols sequence from {:?} to {:?}",
            own_address, neighbor_address
        );

        state
            .send_through_socket(
                own_address.clone(),
                Box::new(neighbor_address),
                Box::new(TestMessage::new(
                    Arc::new(TestMessageType::new()),
                    Some(protocol_id),
                )),
            )
            .await
            .unwrap();
        info!("sent message");
    }

    async fn reconciliation_mechanism(
        state: Arc<DefaultNodeState>,
        port: NodeAddress,
        protocol_id: u64,
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
            let state_clone = state.clone();
            let port_clone = port.clone();
            let info_clone = info.clone();
            let protocol_id_clone = protocol_id;

            let rt_handle = {
                let guard = RUNTIME.read().unwrap();
                Arc::clone(&*guard)
            };

            rt_handle
                .spawn(Box::new(move || {
                    let state = state_clone.clone();
                    let port = port_clone.clone();
                    let info = info_clone.clone();
                    let protocol_id = protocol_id_clone;
                    Box::pin(async move {
                        RIBLT::sending_symbols_sequence(state, port, info, protocol_id).await;
                        Ok(())
                    })
                }))
                .await;
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
        info!("Running RIBLT task");
    }
}

#[derive(Default)]
pub struct RIBLTDeserializer {}

impl ProtocolDeserializer for RIBLTDeserializer {
    fn deserialize(&self, bytes: Vec<u8>) -> Arc<dyn Message> {
        Arc::new(TestMessage::new(Arc::new(TestMessageType::new()), None))
    }
}

pub enum RIBLTMessageTypeValues {
    SYMBOL,
}

impl MessageTypeValues for RIBLTMessageTypeValues {}

pub struct RIBLTMessageType {
    value: RIBLTMessageTypeValues,
}

impl RIBLTMessageType {
    pub fn new() -> Self {
        Self {
            value: RIBLTMessageTypeValues::SYMBOL,
        }
    }
}

impl MessageType for RIBLTMessageType {
    fn value(&self) -> Box<dyn MessageTypeValues> {
        Box::new(RIBLTMessageTypeValues::SYMBOL)
    }
}

#[derive(Debug, Clone)]
pub struct RIBLTSymbol {
    key: String,
    value: Vec<u8>,
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

        self.state
            .add_socket_task_and_create(
                NodeSocketRouteId::new(self.port.clone(), protocol_id),
                Box::new(RibltTask::new()),
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

                        Box::pin(async move {
                            Self::reconciliation_mechanism(state, port, protocol_id).await
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
    fn state(&self) {}
}
