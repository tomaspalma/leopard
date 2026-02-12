use connection::node::{NodeSocket, NodeSocketTask, NodeSocketTaskMetadata, port::ConnectionInfo};
use membership::{Membership, MembershipNeighbor, MembershipNeighbors};
use state::node::NodeState;

pub trait Protocol<S, T, M, R, N, MN, CI, CV>
where
    S: NodeState<T, M, N, R, MN, CI, CV>,
    T: NodeSocketTask<M>,
    M: NodeSocketTaskMetadata,
    R: MembershipNeighbors<MN>,
    N: Membership<R, MN>,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV>,
    CV: Sized,
{
    fn init(&mut self);
}
