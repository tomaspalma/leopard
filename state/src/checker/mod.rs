pub mod local;

use async_trait::async_trait;

pub struct ReconciliationMismatch {
    pub key: String,
    pub node_values: Vec<Option<String>>,
}

pub enum ReconciliationCheckResult {
    Reconciled,
    NotReconciled(Vec<ReconciliationMismatch>),
}

#[async_trait]
pub trait ReconciliationCheckerStrategy: Send + Sync {
    async fn check(&self) -> ReconciliationCheckResult;
}

#[async_trait]
pub trait ReconciliationChecker: Send + Sync {
    fn strategy(&self) -> &Box<dyn ReconciliationCheckerStrategy>;
    async fn check(&self) -> ReconciliationCheckResult;
}
