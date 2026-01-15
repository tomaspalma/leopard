use connection::node::{NodeSocket, NodeSocketTask, NodeSocketTaskMetadata};
use membership::{Membership, MembershipNeighbor, MembershipNeighbors};
use state::node::NodeState;

pub trait Protocol<S, T, M, R, N, MN>
where
    S: NodeState<T, M, N, R, MN>,
    T: NodeSocketTask<M>,
    M: NodeSocketTaskMetadata,
    R: MembershipNeighbors,
    N: Membership<R, MN>,
    MN: MembershipNeighbor,
{
    fn init(&mut self);
}
