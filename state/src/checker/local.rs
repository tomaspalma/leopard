use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;

use crate::storage::state::DataState;

use super::{
    ReconciliationCheckResult, ReconciliationChecker, ReconciliationCheckerStrategy,
    ReconciliationMismatch,
};

pub struct LocalReconciliationCheckerStrategy {
    nodes: Vec<Arc<dyn DataState + Send + Sync>>,
}

impl LocalReconciliationCheckerStrategy {
    pub fn new(nodes: Vec<Arc<dyn DataState + Send + Sync>>) -> Self {
        Self { nodes }
    }
}

pub struct LocalReconciliationChecker {
    strategy: Box<dyn ReconciliationCheckerStrategy>,
}

impl LocalReconciliationChecker {
    pub fn new(nodes: Vec<Arc<dyn DataState + Send + Sync>>) -> Self {
        Self {
            strategy: Box::new(LocalReconciliationCheckerStrategy::new(nodes)),
        }
    }
}

#[async_trait]
impl ReconciliationChecker for LocalReconciliationChecker {
    fn strategy(&self) -> &Box<dyn ReconciliationCheckerStrategy> {
        &self.strategy
    }

    async fn check(&self) -> ReconciliationCheckResult {
        self.strategy.check().await
    }
}

#[async_trait]
impl ReconciliationCheckerStrategy for LocalReconciliationCheckerStrategy {
    async fn check(&self) -> ReconciliationCheckResult {
        let snapshots: Vec<std::collections::HashMap<String, String>> = self
            .nodes
            .iter()
            .map(|node| {
                node.items()
                    .into_iter()
                    .map(|item| (item.key().to_string(), item.value().to_string()))
                    .collect()
            })
            .collect();

        let all_keys: HashSet<String> = snapshots
            .iter()
            .flat_map(|map| map.keys().cloned())
            .collect();

        let mut mismatches = Vec::new();

        for key in &all_keys {
            let node_values: Vec<Option<String>> = snapshots
                .iter()
                .map(|snap| snap.get(key).cloned())
                .collect();

            let first = &node_values[0];
            if node_values.iter().any(|v| v != first) {
                mismatches.push(ReconciliationMismatch {
                    key: key.clone(),
                    node_values,
                });
            }
        }

        if mismatches.is_empty() {
            ReconciliationCheckResult::Reconciled
        } else {
            ReconciliationCheckResult::NotReconciled(mismatches)
        }
    }
}
