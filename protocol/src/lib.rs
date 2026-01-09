use connection::node::{NodeSocket, NodeSocketTask, NodeSocketTaskMetadata};
use state::node::NodeState;

pub trait Protocol<S, T, M> 
where 
    S: NodeState<T, M>,
    T: NodeSocketTask<M>,
    M: NodeSocketTaskMetadata
{
    fn init(&mut self);
}
