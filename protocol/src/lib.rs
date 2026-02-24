use async_trait::async_trait;

use uuid::Uuid;

use connection::node::{NodeSocketTaskMetadata, PeriodicNodeSocketTask, port::ConnectionInfo};
use connection::route::{RouteHandler, RouteStorage, RouteTask};
use membership::{Membership, MembershipNeighbor, MembershipNeighbors};
use message::MessageType;
use runtime::time::PeriodTimeUnit;
use state::node::NodeState;

#[async_trait]
pub trait Protocol<S, T, M, R, N, MN, CI, CV, PTU, PT, MType, RHandler, RStorage>
where
    S: NodeState<T, M, N, R, MN, CI, CV, PTU, PT, MType, RHandler, RStorage>,
    T: RouteTask,
    M: NodeSocketTaskMetadata,
    R: MembershipNeighbors<MN>,
    N: Membership<R, MN>,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV>,
    CV: Sized,
    PTU: PeriodTimeUnit + Send + Sync,
    PT: PeriodicNodeSocketTask<PTU>,
    MType: MessageType,
    RHandler: RouteHandler<MType, RStorage> + Send + Sync,
    RStorage: RouteStorage,
{
    fn id(&self) -> String {
        Uuid::new_v4().to_string()
    }
    async fn init(&mut self);
}
