use connection::node::port::NodeAddress;
use connection::{node::id::DefaultNodeIdentifier, route::default::DefaultRouteHandler};
use membership_protocols::DefaultMembershipProtocol;
use reconciliation::merkle_tree::protocol::MerkleTreeReconciliationProtocol;
use runtime::{RUNTIME, Task};

use services::http::NodeHTTPService;
use state::node::{DefaultNodeState, NodeState};

use config::node::DefaultNodeConfig;
use node::Node;

use state::storage::state::DefaultDataState;
use tracing::info;
use tracing_subscriber;

use std::sync::Arc;

use runtime::metrics::csv::CsvRecorder;
use metrics::set_global_recorder;
use std::time::Duration;

pub async fn default_three_nodes() {
    tracing_subscriber::fmt::init();

    let recorder = CsvRecorder::new();
    recorder.clone().start_exporter(
        "./metrics_output".to_string(),
        Duration::from_secs(5)
    );
    set_global_recorder(recorder).expect("Failed to set global metrics recorder");

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

            node.add_protocol(Box::new(DefaultMembershipProtocol::new()));
            node.add_protocol(Box::new(
                MerkleTreeReconciliationProtocol::new(node_state.clone(), NodeAddress::new("127.0.0.1".to_string(), 9000))
            ));

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

            node.add_protocol(Box::new(DefaultMembershipProtocol::new()));
            node.add_protocol(Box::new(MerkleTreeReconciliationProtocol::new(
                node_state.clone(),
                NodeAddress::new("127.0.0.1".to_string(), 9001)
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

            node.add_protocol(Box::new(DefaultMembershipProtocol::new()));
            node.add_protocol(Box::new(
                MerkleTreeReconciliationProtocol::new(node_state.clone(), NodeAddress::new("127.0.0.1".to_string(), 9002))
            ));

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
