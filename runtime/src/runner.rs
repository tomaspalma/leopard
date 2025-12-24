use crate::Runtime;
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
    pub runtime: Runtime,
    pub nodeRunner: NodeRunner,
}

impl Runner {
    pub fn new(runtime: Runtime) -> Self {
        Runner { 
            runtime,
            nodeRunner: Some(NodeRunner::new()),
        }
    }

    pub fn node() {
        NodeRunner::node()
    }
}