use crate::Runtime;
use crate::builder::runner::RunnerBuilder;

use node::Node;

pub struct NodeRunner {
    pub node: Option<Node>,
}

impl NodeRunner {
    pub fn new() -> Self {
        NodeRunner {
            node: None,
        }
    }

    pub fn node() -> Node {
        Node::new()
    }
}

pub struct Runner {
    pub runtime: Box<dyn Runtime>,
    pub nodeRunner: NodeRunner,
}

impl Runner {
    pub fn new(runtime: Box<dyn Runtime>) -> Self {
        Runner { 
            runtime,
            nodeRunner: NodeRunner::new(),
        }
    }

    pub fn builder() -> RunnerBuilder {
        RunnerBuilder::new()
    }

    pub fn node() -> Node {
        NodeRunner::node()
    }
}
