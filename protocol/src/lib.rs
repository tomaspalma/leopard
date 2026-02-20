use async_trait::async_trait;

use connection::node::{
    NodeSocket, NodeSocketTask, NodeSocketTaskMetadata, PeriodicNodeSocketTask,
    port::ConnectionInfo,
};
use connection::route::RouteHandler;
use membership::{Membership, MembershipNeighbor, MembershipNeighbors};
use message::MessageType;
use runtime::time::PeriodTimeUnit;
use state::node::NodeState;

#[async_trait]
pub trait Protocol<S, T, M, R, N, MN, CI, CV, PTU, PT, MType, RHandler>
where
    S: NodeState<T, M, N, R, MN, CI, CV, PTU, PT, MType, RHandler>,
    T: NodeSocketTask<M>,
    M: NodeSocketTaskMetadata,
    R: MembershipNeighbors<MN>,
    N: Membership<R, MN>,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV>,
    CV: Sized,
    PTU: PeriodTimeUnit + Send + Sync,
    PT: PeriodicNodeSocketTask<PTU>,
    MType: MessageType,
    RHandler: RouteHandler<MType> + Send + Sync,
{
    async fn init(&mut self);
}
