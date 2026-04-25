use connection::node::port::NodeAddress;
use connection::{node::id::DefaultNodeIdentifier, route::default::DefaultRouteHandler};
use membership_protocols::DefaultMembershipProtocol;
use reconciliation::checker::local::LocalReconciliationChecker;
use reconciliation::checker::ReconciliationCheckResult;
use reconciliation::merkle_tree::protocol::MerkleTreeReconciliationProtocol;
use reconciliation::rbf_riblt::RbfRibltProtocol;
use reconciliation::riblt::RIBLT;
use runtime::{RUNTIME, Task};

use services::http::NodeHTTPService;
use state::checker::ReconciliationChecker;
use state::node::{DefaultNodeState, NodeState};

use config::node::DefaultNodeConfig;
use node::Node;

use state::storage::state::{DataState, DefaultDataState};
use tracing::{Instrument, info};

use std::sync::Arc;

pub fn default_task(
    ip: String,
    port: u16,
    http_port: u16,
    storage: Arc<dyn DataState + Send + Sync>,
    checker: Arc<dyn ReconciliationChecker>,
    protocol: String,
) -> Box<Task> {
    Box::new(move || {
        let ip_clone = ip.clone();
        let protocol_clone = protocol.clone();
        let storage_clone = storage.clone();
        let checker_clone = checker.clone();
        Box::pin(async move {
            let node_name = format!("{}:{}", ip_clone, port);
            let span = tracing::info_span!("node", name = %node_name);

            async move {
                info!("Starting node at {}:{}", ip_clone, port);
                let config = Arc::new(DefaultNodeConfig::new());
                let node_id = DefaultNodeIdentifier::new(
                    node_name.clone(),
                    NodeAddress::new(ip_clone.clone(), port),
                );

                let node_state = Arc::new(DefaultNodeState::new(
                    config.clone(),
                    Arc::new(node_id),
                    Arc::new(DefaultRouteHandler::new()),
                ));

                node_state.register_storage("default".to_string(), storage_clone);
                node_state.set_reconciliation_checker(checker_clone);

                let mut node = Node::new(
                    node_state.clone(),
                    config.clone(),
                    Box::new(DefaultNodeIdentifier::new(
                        node_name.clone(),
                        NodeAddress::new(ip_clone.clone(), port),
                    )),
                );

                node.add_protocol(Box::new(DefaultMembershipProtocol::new()));

                match protocol_clone.as_str() {
                    "merkle" => {
                        node.add_protocol(Box::new(MerkleTreeReconciliationProtocol::new(
                            node_state.clone(),
                            NodeAddress::new(ip_clone.clone(), port),
                        )));
                    }
                    "riblt" => {
                        node.add_protocol(Box::new(RIBLT::new(
                            node_state.clone(),
                            NodeAddress::new(ip_clone.clone(), port),
                        )));
                    }
                    "rbf_riblt" => {
                        node.add_protocol(Box::new(RbfRibltProtocol::new(
                            node_state.clone(),
                            NodeAddress::new(ip_clone.clone(), port),
                        )));
                    }
                    _ => panic!("Unknown protocol: {}", protocol_clone),
                }

                node.add_service(Arc::new(NodeHTTPService::new(
                    NodeAddress::new(ip_clone.clone(), http_port),
                    node_state.clone(),
                )));

                node.init().await.unwrap();

                Ok(())
            }
            .instrument(span)
            .await
        })
    })
}

pub async fn custom_nodes(
    node_type: String,
    protocol: String,
    nodes: Vec<String>,
    exit_on_reconciliation: bool,
) {
    let node_count = nodes.len();

    let node_specs: Vec<(String, u16, u16, String)> = nodes
        .iter()
        .map(|node_str| {
            let parts: Vec<&str> = node_str.split(',').collect();
            if parts.len() != 4 {
                panic!(
                    "Invalid node format: {}. Expected ip,port,http_port,dataset",
                    node_str
                );
            }
            (
                parts[0].to_string(),
                parts[1].parse().expect("Invalid port"),
                parts[2].parse().expect("Invalid http_port"),
                parts[3].to_string(),
            )
        })
        .collect();

    let storages: Vec<Arc<dyn DataState + Send + Sync>> =
        futures::future::join_all(node_specs.iter().map(|(_, _, _, dataset)| {
            let dataset = dataset.clone();
            async move {
                Arc::new(DefaultDataState::new(dataset).await) as Arc<dyn DataState + Send + Sync>
            }
        }))
        .await;

    let checker: Arc<dyn ReconciliationChecker> =
        Arc::new(LocalReconciliationChecker::new(storages.clone()));

    for ((ip, port, http_port, _), storage) in node_specs.into_iter().zip(storages.into_iter()) {
        let task = match node_type.as_str() {
            "default" => default_task(
                ip,
                port,
                http_port,
                storage,
                checker.clone(),
                protocol.clone(),
            ),
            _ => panic!("Unknown node type: {}", node_type),
        };
        RUNTIME.write().unwrap().add_task(task).unwrap();
    }

    RUNTIME.write().unwrap().init().unwrap();

    if exit_on_reconciliation {
        runtime::metrics::csv::set_expected_pairs(node_count * (node_count - 1));
        runtime::metrics::csv::shutdown_complete_notify().notified().await;

        match checker.check().await {
            ReconciliationCheckResult::Reconciled => {
                info!("Reconciliation check passed: all nodes are in sync");
            }
            ReconciliationCheckResult::NotReconciled(mismatches) => {
                tracing::warn!(
                    "Reconciliation check failed: {} key(s) out of sync",
                    mismatches.len()
                );
                for m in &mismatches {
                    let values: Vec<String> = m
                        .node_values
                        .iter()
                        .enumerate()
                        .map(|(i, v)| format!("node {}: {}", i, v.as_deref().unwrap_or("missing")))
                        .collect();
                    tracing::warn!("  key '{}': {}", m.key, values.join(", "));
                }
            }
        }
    } else {
        std::future::pending::<()>().await;
    }
}
