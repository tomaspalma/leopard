use replication::protocol::HintedHandoffReplicationProtocol;
use runtime::{Runtime, TokioRuntime, runner::Runner};
use node::{Node, state::DefaultNodeState, connection::port::NodePort};

use std::sync::Arc;

fn main() {
    let mut runtime = TokioRuntime::new(
    Some(Box::new(|| { 
        Box::pin(async { 
            let node_state = Arc::new(DefaultNodeState::new());


            let mut node = Node::new_with_state(node_state.clone());
            node.add_protocol(Box::new(HintedHandoffReplicationProtocol::new(
                        node_state.clone(),
                NodePort::new(9000)
            )));

            node.init().await.unwrap();

            Ok(())
        })
    }))
    );
   
    runtime.init();
}

