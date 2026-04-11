use std::{collections::HashSet, sync::Arc};

use riblt::RatelessIBLT;
use tokio::sync::Mutex;
use tracing::info;

use async_trait::async_trait;
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
use dashmap::DashMap;
use membership::{Membership, MembershipNeighbor, MembershipNeighbors};
use message::Message;
use protocol::{deserializer::ProtocolDeserializer, Protocol};
use runtime::{
    spawn,
    time::{PeriodTimeUnit, TokioPeriodTimeUnit},
};
use state::{node::NodeState, storage::StorageAction};

use crate::{
    riblt::{
        messages::RIBLTSymbol, receiver::ReceiveNeighborSymbolsTask, RIBLTDeserializer, RIBLT,
    },
    ReconciliationProtocol,
};

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
        let sending_states = self.sending_states.clone();
        let receiving_states = self.receiving_states.clone();

        if let Some(storage) = self.state.get_storage("default".to_string()) {
            let items = storage.items();
            let mut symbols = HashSet::new();

            Self::update_symbols(&mut symbols, items);
        } else {
            info!("No default storage found");
        }

        let state_clone = self.state.clone();
        self.state
            .add_socket_task_and_create(
                NodeSocketRouteId::new(self.port.clone(), protocol_id),
                Arc::new(ReceiveNeighborSymbolsTask::new(
                    state_clone.node_identifier(),
                    state_clone,
                    self.sending_states.clone(),
                    self.receiving_states.clone(),
                )),
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
                        let sending_states = sending_states.clone();
                        let receiving_states = receiving_states.clone();

                        Box::pin(async move {
                            Self::reconciliation_mechanism(
                                state,
                                port,
                                protocol_id,
                                sending_states,
                                
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
        for r in self.sending_states.iter() {
            info!("  {:?}: {:?}", r.key(), r.value().state);
        }
    }
}
