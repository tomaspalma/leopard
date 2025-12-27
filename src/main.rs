use replication::protocol::HintedHandoffReplicationProtocol;
use runtime::{Runtime, TokioRuntime, runner::Runner};
use node::{Node, state::DefaultNodeState, connection::port::NodePort};

fn main() {
    let mut runtime = TokioRuntime::new(
        Some(Box::new(|| {
            let mut node = Node::new();

            let node_state = DefaultNodeState::new();

            node.add_protocol(Box::new(HintedHandoffReplicationProtocol::new(
                        Box::new(node_state), NodePort::new(9000)
                        )
                    )
                );

            node.init();

            Ok(())
        }))
    );

    runtime.init();
}

