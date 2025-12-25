use crate::builder::node::NodeBuilder;
use node::Node;

pub struct RunnerBuilder {
    nodes: Vec<Node>
}

impl RunnerBuilder {
    pub fn new() -> Self {
        RunnerBuilder {
            nodes: vec![],
        }
    }

    pub fn node(&self) -> NodeBuilder {
        NodeBuilder::new()
    }

    pub fn build() {

    }
}

