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

use std::sync::Arc;

pub fn default_task(ip: String, port: u16, http_port: u16, dataset: String) -> Box<Task> {
    Box::new(move || {
        let ip_clone = ip.clone();
        let dataset_clone = dataset.clone();
        Box::pin(async move {
            info!("Starting node at {}:{}", ip_clone, port);
            let config = Arc::new(DefaultNodeConfig::new());
            let node_id = DefaultNodeIdentifier::new(NodeAddress::new(ip_clone.clone(), port));

            let node_state = Arc::new(DefaultNodeState::new(
                config.clone(),
                Arc::new(node_id),
                Arc::new(DefaultRouteHandler::new()),
            ));

            node_state.register_storage(
                "default".to_string(),
                Arc::new(DefaultDataState::new(dataset_clone).await),
            );

            let mut node = Node::new(
                node_state.clone(),
                config.clone(),
                Box::new(DefaultNodeIdentifier::new(NodeAddress::new(
                    ip_clone.clone(),
                    port,
                ))),
            );

            node.add_protocol(Box::new(DefaultMembershipProtocol::new()));
            node.add_protocol(Box::new(MerkleTreeReconciliationProtocol::new(
                node_state.clone(),
                NodeAddress::new(ip_clone.clone(), port),
            )));

            node.add_service(Arc::new(NodeHTTPService::new(
                NodeAddress::new(ip_clone.clone(), http_port),
                node_state.clone(),
            )));

            node.init().await.unwrap();

            Ok(())
        })
    })
}

use reconciliation::riblt::RIBLT;

pub fn riblt_task(ip: String, port: u16, http_port: u16, dataset: String) -> Box<Task> {
    Box::new(move || {
        let ip_clone = ip.clone();
        let dataset_clone = dataset.clone();
        Box::pin(async move {
            info!("Starting node at {}:{}", ip_clone, port);
            let config = Arc::new(DefaultNodeConfig::new());
            let node_id = DefaultNodeIdentifier::new(NodeAddress::new(ip_clone.clone(), port));

            let node_state = Arc::new(DefaultNodeState::new(
                config.clone(),
                Arc::new(node_id),
                Arc::new(DefaultRouteHandler::new()),
            ));

            node_state.register_storage(
                "default".to_string(),
                Arc::new(DefaultDataState::new(dataset_clone).await),
            );

            let mut node = Node::new(
                node_state.clone(),
                config.clone(),
                Box::new(DefaultNodeIdentifier::new(NodeAddress::new(
                    ip_clone.clone(),
                    port,
                ))),
            );

            node.add_protocol(Box::new(DefaultMembershipProtocol::new()));
            node.add_protocol(Box::new(RIBLT::new(
                node_state.clone(),
                NodeAddress::new(ip_clone.clone(), port),
            )));

            node.add_service(Arc::new(NodeHTTPService::new(
                NodeAddress::new(ip_clone.clone(), http_port),
                node_state.clone(),
            )));

            node.init().await.unwrap();

            Ok(())
        })
    })
}

pub async fn custom_nodes(protocol: String, nodes: Vec<String>) {
    for node_str in nodes {
        let parts: Vec<&str> = node_str.split(',').collect();
        
        if parts.len() != 4 {
            panic!(
                "Invalid node format: {}. Expected ip,port,http_port,dataset",
                node_str
            );
        }

        let ip = parts[0].to_string();
        let port: u16 = parts[1].parse().expect("Invalid port");
        let http_port: u16 = parts[2].parse().expect("Invalid http_port");
        let dataset = parts[3].to_string();

        let task_node = match protocol.as_str() {
            "default" | "merkle" => default_task(ip, port, http_port, dataset),
            "riblt" => riblt_task(ip, port, http_port, dataset),
            _ => panic!("Unknown protocol: {}", protocol),
        };

        RUNTIME.write().unwrap().add_task(task_node).unwrap();
    }

    RUNTIME.write().unwrap().init().unwrap();

    loop {}
}
