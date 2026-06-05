use std::sync::Arc;

use state::checker::ReconciliationChecker;
use state::checker::local::LocalReconciliationChecker;
use state::storage::state::DataState;

#[derive(Clone)]
pub enum CheckerChoice {
    Local,
}

/// Builder for the reconciliation checker. Select a variant (e.g.
/// [`CheckerBuilder::local`]) and hand it to [`crate::NodeBuilder::checker`];
/// the node builder resolves it via [`CheckerBuilder::build`] and materializes
/// the concrete checker against the node storages at build time.
pub struct CheckerBuilder {
    choice: Option<CheckerChoice>,
}

impl CheckerBuilder {
    pub fn new() -> Self {
        Self { choice: None }
    }

    pub fn local(mut self) -> Self {
        self.choice = Some(CheckerChoice::Local);
        self
    }

    pub fn build(self) -> Result<CheckerChoice, String> {
        self.choice
            .ok_or_else(|| "no checker selected in CheckerBuilder".to_string())
    }
}

impl Default for CheckerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub fn build_checker(
    choice: &CheckerChoice,
    storages: Vec<Arc<dyn DataState + Send + Sync>>,
) -> Arc<dyn ReconciliationChecker> {
    match choice {
        CheckerChoice::Local => Arc::new(LocalReconciliationChecker::new(storages)),
    }
}
