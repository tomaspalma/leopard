use dsl::{CheckerBuilder, NodeBuilder, ProtocolChoice, ServiceBuilder};
use reconciliation::checker::ReconciliationCheckResult;
use runtime::RUNTIME;

use metrics::{counter, gauge};
use runtime::metrics::experiment::get_context;

pub async fn custom_nodes(
    node_type: String,
    protocol: String,
    nodes: Vec<String>,
    exit_on_reconciliation: bool,
) {
    if node_type != "default" {
        panic!("Unknown node type: {}", node_type);
    }

    let protocol_choice = match protocol.as_str() {
        "merkle" => ProtocolChoice::Merkle,
        "riblt" => ProtocolChoice::Riblt,
        "rbf_riblt" => ProtocolChoice::RbfRiblt,
        "rf_riblt" => ProtocolChoice::RfRiblt,
        _ => panic!("Unknown protocol: {}", protocol),
    };

    let node_count = nodes.len();

    let mut builder = NodeBuilder::new();

    for node_str in &nodes {
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

        builder = builder
            .node()
            .addr(ip)
            .port(port)
            .dataset(dataset)
            .protocol(protocol_choice.clone())
            .service(ServiceBuilder::http().port(http_port));
    }

    let result = builder
        .checker(CheckerBuilder::new().local())
        .build()
        .await
        .expect("Failed to build nodes");

    for task in result.tasks {
        RUNTIME.write().unwrap().add_task(task).unwrap();
    }

    RUNTIME.write().unwrap().init().unwrap();

    if exit_on_reconciliation {
        runtime::metrics::csv::set_expected_pairs(node_count * (node_count - 1));
        runtime::metrics::csv::shutdown_complete_notify().notified().await;

        let context = get_context();
        let ctx_labels = [
            ("protocol", protocol.clone()),
            ("run_id", context.run_id().to_string()),
            ("trial", context.trial().to_string()),
            ("similarity", context.similarity().to_string()),
        ];

        match result.checker.check().await {
            ReconciliationCheckResult::Reconciled => {
                tracing::info!("Reconciliation check passed: all nodes are in sync");
                gauge!(
                    "reconciliation_correctness",
                    "protocol" => ctx_labels[0].1.clone(),
                    "run_id" => ctx_labels[1].1.clone(),
                    "trial" => ctx_labels[2].1.clone(),
                    "similarity" => ctx_labels[3].1.clone()
                )
                .set(1.0);
                counter!(
                    "reconciliation_check_passed",
                    "protocol" => ctx_labels[0].1.clone(),
                    "run_id" => ctx_labels[1].1.clone(),
                    "trial" => ctx_labels[2].1.clone(),
                    "similarity" => ctx_labels[3].1.clone()
                )
                .increment(1);
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
                gauge!(
                    "reconciliation_correctness",
                    "protocol" => ctx_labels[0].1.clone(),
                    "run_id" => ctx_labels[1].1.clone(),
                    "trial" => ctx_labels[2].1.clone(),
                    "similarity" => ctx_labels[3].1.clone()
                )
                .set(0.0);
                gauge!(
                    "reconciliation_mismatch_keys",
                    "protocol" => ctx_labels[0].1.clone(),
                    "run_id" => ctx_labels[1].1.clone(),
                    "trial" => ctx_labels[2].1.clone(),
                    "similarity" => ctx_labels[3].1.clone()
                )
                .set(mismatches.len() as f64);
                counter!(
                    "reconciliation_check_failed",
                    "protocol" => ctx_labels[0].1.clone(),
                    "run_id" => ctx_labels[1].1.clone(),
                    "trial" => ctx_labels[2].1.clone(),
                    "similarity" => ctx_labels[3].1.clone()
                )
                .increment(1);
            }
        }

        let output_dir = format!("./metrics_output/{}", context.run_id());
        runtime::metrics::csv::flush_untagged_metrics(&output_dir).await;
    } else {
        std::future::pending::<()>().await;
    }
}
