use replication::protocol::HintedHandoffReplicationProtocol;
use runtime::{Runtime, TokioRuntime, runner::Runner};
use node::{Node, state::DefaultNodeState};

fn main() {
    let mut runtime = TokioRuntime::new(
        Some(Box::new(|| {
            let mut node = Node::new();

            let node_state = DefaultNodeState::new();

            node.add_protocol(Box::new(HintedHandoffReplicationProtocol::new(Box::new(node_state))));

            println!("Hello, world!");
            Ok(())
        }))
    );

    runtime.init();

    // This is a small example of the final result of instantiating a system, using a custom DSL
    // with the builder pattern.
    // let _ = Runner::builder()
    //     .node()
    //         .protocol();
}

