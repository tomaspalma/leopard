use std::sync::Arc;

use async_trait::async_trait;
use protocol::Protocol;
use state::node::{DefaultNodeState, NodeState};

use connection::{
    node::{
        default::{
            DefaultNodeSocketTask, DefaultNodeSocketTaskMetadata, PeriodicDefaultNodeSocketTask,
        },
        port::{ConnectionInfo, NodeAddress},
        NodeSocketTaskMetadata, PeriodicNodeSocketTask,
    },
    request::handler::default::{TestMessage, TestMessageType},
    route::{RouteHandler, RouteStorage, RouteTask},
};
use membership::{Membership, MembershipNeighbor, MembershipNeighbors};
use runtime::time::{PeriodTimeUnit, TokioPeriodTimeUnit};

use crate::ReconciliationProtocol;

pub struct RIBLT {
    state: Arc<DefaultNodeState>,
    port: NodeAddress,
}

impl RIBLT {
    pub fn new(state: Arc<DefaultNodeState>, port: NodeAddress) -> Self {
        Self { state, port }
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
    async fn init(&mut self) {
        let state_handle = self.state.clone();
        let port_for_closure = self.port.clone();

        self.state
            .add_periodic_socket_task(
                self.port.clone(),
                Arc::new(PeriodicDefaultNodeSocketTask::new(
                    Arc::new(DefaultNodeSocketTaskMetadata::new(String::new())),
                    Arc::new(move || {
                        let state = state_handle.clone();
                        let port_clone = port_for_closure.clone();

                        Box::pin(async move {
                            println!("Running RIBLT");

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
                                state
                                    .send_through_socket(
                                        port_clone.clone(),
                                        Box::new(info),
                                        Box::new(TestMessage::new(
                                            Arc::new(TestMessageType::new()),
                                        )),
                                    )
                                    .await
                                    .unwrap();
                                println!("sent message");
                            }

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
