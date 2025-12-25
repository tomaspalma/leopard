use node::Node;

pub struct NodeBuilder {
    node: Node,
}

impl NodeBuilder {
    pub fn new() -> Self {
        NodeBuilder {
            node: Node::new(),
        }
    }
}

