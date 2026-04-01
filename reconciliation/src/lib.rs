use async_trait::async_trait;

use connection::{
    node::{port::ConnectionInfo, NodeSocketTaskMetadata, PeriodicNodeSocketTask},
    route::{RouteHandler, RouteStorage, RouteTask},
};
use membership::{Membership, MembershipNeighbor, MembershipNeighbors};
use protocol::Protocol;
use runtime::time::PeriodTimeUnit;
use state::node::NodeState;

pub mod algorithms;
pub mod hybrid;
pub mod pbs;
pub mod pin_sketch;
pub mod riblt;
pub mod merkle_tree;

#[async_trait]
pub trait ReconciliationProtocol<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>:
    Protocol<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
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
    fn state(&self);
}
