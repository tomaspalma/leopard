use node::Node;

use crate::builder::{Builder, runner::RunnerBuilder};

pub struct NodeBuilder<'a> {
    node: Node,
    callback: Option<Box<dyn FnMut(Node) -> &'a mut RunnerBuilder>>,
}

impl<'a> Builder<Node> for NodeBuilder<'a> {
    fn build(self) -> Node {
        self.node
    }
}

impl<'a> NodeBuilder<'a > {
    pub fn new() -> Self {
        NodeBuilder {
            node: Node::new(),
            callback: None
        }
    }

    pub fn new_with_callback(callback: Box<dyn Fn(Node) -> &'a mut RunnerBuilder>) -> Self {
        NodeBuilder {
            node: Node::new(),
            callback: Some(callback)
        }
    }

    pub fn node(&self) -> NodeBuilder {
        NodeBuilder::new()
    }

    pub fn protocol(&self) {
    }
}
