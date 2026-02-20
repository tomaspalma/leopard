use connection::node::port::NodePort;
use connection::{node::id::DefaultNodeIdentifier, route::DefaultRouteHandler};
use membership_protocols::DefaultMembershipProtocol;
use replication::protocol::HintedHandoffReplicationProtocol;
use runtime::{Runtime, Task, TokioRuntime};
use state::node::DefaultNodeState;

use config::node::DefaultNodeConfig;
use node::Node;

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
        let runtime_value = runtime_clone.clone();
        Box::pin(async move {
            let config = Arc::new(DefaultNodeConfig::new());
            let node1_id = DefaultNodeIdentifier::new(NodePort::new(9000));
            let node_state = Arc::new(DefaultNodeState::new(
                runtime_value.clone(),
                config.clone(),
                Arc::new(node1_id),
                Arc::new(DefaultRouteHandler::new()),
            ));

            let mut node = Node::new(
                runtime_value.clone(),
                node_state.clone(),
                config.clone(),
                Box::new(DefaultNodeIdentifier::new(NodePort::new(9000))),
            );
            node.add_protocol(Box::new(HintedHandoffReplicationProtocol::new(
                node_state.clone(),
                NodePort::new(9000),
            )));
            node.add_protocol(Box::new(DefaultMembershipProtocol::new()));

            node.init().await.unwrap();

            Ok(())
        })
    });

    runtime.add_task(task).unwrap();

    runtime.init().unwrap();

    loop {}
}
