use connection::node::port::NodeAddress;
use connection::{node::id::DefaultNodeIdentifier, route::default::DefaultRouteHandler};
use membership_protocols::DefaultMembershipProtocol;
use reconciliation::riblt::RIBLT;
use runtime::{RUNTIME, Task};

use services::http::NodeHTTPService;
use state::node::{DefaultNodeState, NodeState};

use config::node::DefaultNodeConfig;
use node::Node;

use state::storage::state::DefaultDataState;
use tracing::info;
use tracing_subscriber;

use std::sync::Arc;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let task_node1: Box<Task> = Box::new(move || {
        Box::pin(async move {
            info!("Starting node 1");
            let config = Arc::new(DefaultNodeConfig::new());
            let node1_id =
                DefaultNodeIdentifier::new(NodeAddress::new("127.0.0.1".to_string(), 9000));

            let node_state = Arc::new(DefaultNodeState::new(
                config.clone(),
                Arc::new(node1_id),
                Arc::new(DefaultRouteHandler::new()),
            ));

            node_state.register_storage(
                "default".to_string(),
                Arc::new(DefaultDataState::new("node1_data.json".to_string()).await),
            );

            let mut node = Node::new(
                node_state.clone(),
                config.clone(),
                Box::new(DefaultNodeIdentifier::new(NodeAddress::new(
                    "127.0.0.1".to_string(),
                    9000,
                ))),
            );

            // node.add_protocol(Box::new(HintedHandoffReplicationProtocol::new(
            //     node_state.clone(),
            //     NodeAddress::new("127.0.0.1".to_string(), 9000),
            // )));
            node.add_protocol(Box::new(DefaultMembershipProtocol::new()));
            node.add_protocol(Box::new(RIBLT::new(
                node_state.clone(),
                NodeAddress::new("127.0.0.1".to_string(), 9000),
            )));

            node.add_service(Arc::new(NodeHTTPService::new(
                NodeAddress::new("127.0.0.1".to_string(), 3000),
                node_state.clone(),
            )));

            node.init().await.unwrap();

            Ok(())
        })
    });

    let task_node2: Box<Task> = Box::new(move || {
        Box::pin(async move {
            info!("Starting node 2");
            let config = Arc::new(DefaultNodeConfig::new());
            let node1_id =
                DefaultNodeIdentifier::new(NodeAddress::new("127.0.0.1".to_string(), 9001));
            let node_state = Arc::new(DefaultNodeState::new(
                config.clone(),
                Arc::new(node1_id),
                Arc::new(DefaultRouteHandler::new()),
            ));
            node_state.register_storage(
                "default".to_string(),
                Arc::new(DefaultDataState::new("node2_data.json".to_string()).await),
            );

            let mut node = Node::new(
                node_state.clone(),
                config.clone(),
                Box::new(DefaultNodeIdentifier::new(NodeAddress::new(
                    "127.0.0.1".to_string(),
                    9001,
                ))),
            );

            // node.add_protocol(Box::new(HintedHandoffReplicationProtocol::new(
            //     node_state.clone(),
            //     NodeAddress::new("127.0.0.1".to_string(), 9001),
            // )));
            node.add_protocol(Box::new(DefaultMembershipProtocol::new()));
            node.add_protocol(Box::new(RIBLT::new(
                node_state.clone(),
                NodeAddress::new("127.0.0.1".to_string(), 9001),
            )));

            node.add_service(Arc::new(NodeHTTPService::new(
                NodeAddress::new("127.0.0.1".to_string(), 3001),
                node_state.clone(),
            )));

            node.init().await.unwrap();

            Ok(())
        })
    });

    let task_node3: Box<Task> = Box::new(move || {
        Box::pin(async move {
            info!("Starting node 3");
            let config = Arc::new(DefaultNodeConfig::new());
            let node1_id =
                DefaultNodeIdentifier::new(NodeAddress::new("127.0.0.1".to_string(), 9002));
            let node_state = Arc::new(DefaultNodeState::new(
                config.clone(),
                Arc::new(node1_id),
                Arc::new(DefaultRouteHandler::new()),
            ));
            node_state.register_storage(
                "default".to_string(),
                Arc::new(DefaultDataState::new("node3_data.json".to_string()).await),
            );

            let mut node = Node::new(
                node_state.clone(),
                config.clone(),
                Box::new(DefaultNodeIdentifier::new(NodeAddress::new(
                    "127.0.0.1".to_string(),
                    9002,
                ))),
            );

            // node.add_protocol(Box::new(HintedHandoffReplicationProtocol::new(
            //     node_state.clone(),
            //     NodeAddress::new("127.0.0.1".to_string(), 9002),
            // )));
            node.add_protocol(Box::new(DefaultMembershipProtocol::new()));
            node.add_protocol(Box::new(RIBLT::new(
                node_state.clone(),
                NodeAddress::new("127.0.0.1".to_string(), 9002),
            )));

            node.add_service(Arc::new(NodeHTTPService::new(
                NodeAddress::new("127.0.0.1".to_string(), 3002),
                node_state.clone(),
            )));

            node.init().await.unwrap();

            Ok(())
        })
    });

    RUNTIME.write().unwrap().add_task(task_node1).unwrap();
    RUNTIME.write().unwrap().add_task(task_node2).unwrap();
    RUNTIME.write().unwrap().add_task(task_node3).unwrap();

    RUNTIME.write().unwrap().init().unwrap();

    loop {}
}
