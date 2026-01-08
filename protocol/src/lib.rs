use connection::node::{NodeSocketTask, NodeSocket};
use state::node::NodeState;

pub trait Protocol<S, T> 
where 
    S: NodeState<T>,
    T: NodeSocketTask
{
    fn init(&mut self);
}
