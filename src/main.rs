use replication::protocol::HintedHandoffReplicationProtocol;
use runtime::{Runtime, TokioRuntime, Task};
use state::node::DefaultNodeState;
use connection::node::port::NodePort;

use node::{Node};

use tracing::info;
use tracing_subscriber;

use std::sync::Arc;

#[tokio::main]
async fn main() {
    tracing::subscriber::with_default(tracing_subscriber::fmt().finish(), || {
        info!("Starting logging");
    });

    let runtime = Arc::new(TokioRuntime::new());
    
    let runtime_clone = runtime.clone();
    let task: Box<Task> = Box::new(move || {
        let value = runtime_clone.clone();

        Box::pin(async move {
            let node_state = Arc::new(DefaultNodeState::new());


            let mut node = Node::new_with_state(node_state.clone(), value);
            node.add_protocol(Box::new(HintedHandoffReplicationProtocol::new(
                        node_state.clone(),
                NodePort::new(9000)
            )));

            node.init().await.unwrap();

            Ok(())
        })
    });

    runtime.add_task(task).unwrap();

    runtime.init().unwrap();

    loop {

    }
}
