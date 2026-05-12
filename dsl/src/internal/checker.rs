use std::sync::Arc;

use state::checker::ReconciliationChecker;
use state::checker::local::LocalReconciliationChecker;
use state::storage::state::DataState;

pub trait CheckerReceiver {
    fn set_checker(&mut self, choice: CheckerChoice);
}

#[derive(Clone)]
pub enum CheckerChoice {
    Local,
}

pub struct CheckerEntry<B: CheckerReceiver> {
    pub(crate) parent: B,
}

impl<B: CheckerReceiver> CheckerEntry<B> {
    pub fn local(mut self) -> B {
        self.parent.set_checker(CheckerChoice::Local);
        self.parent
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
