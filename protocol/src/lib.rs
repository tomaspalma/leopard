use async_trait::async_trait;

use rand::RngExt;

use connection::node::{NodeSocketTaskMetadata, PeriodicNodeSocketTask, port::ConnectionInfo};
use connection::route::{RouteHandler, RouteStorage, RouteTask};
use membership::{Membership, MembershipNeighbor, MembershipNeighbors};
use runtime::time::PeriodTimeUnit;
use state::node::NodeState;

pub struct ProtocolIDGenerator {}

impl ProtocolIDGenerator {
    pub fn generate() -> u64 {
        let mut rng = rand::rng();

        rng.random()
    }
}

#[async_trait]
pub trait Protocol<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
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
    fn id(&self) -> u64;
    async fn init(&mut self);
}
