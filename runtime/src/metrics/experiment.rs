use std::sync::OnceLock;

static CONTEXT: OnceLock<ExperimentContext> = OnceLock::new();

#[derive(Clone, Debug)]
pub struct ExperimentContext {
    run_id: String,
    trial: String,
    similarity: String,
}

impl ExperimentContext {
    pub fn new(run_id: String, trial: String, similarity: String) -> Self {
        Self {
            run_id,
            trial,
            similarity,
        }
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn trial(&self) -> &str {
        &self.trial
    }

    pub fn similarity(&self) -> &str {
        &self.similarity
    }
}

pub fn set_context(context: ExperimentContext) -> Result<(), ExperimentContext> {
    CONTEXT.set(context)
}

pub fn get_context() -> ExperimentContext {
    CONTEXT.get().cloned().unwrap_or_else(|| {
        ExperimentContext::new(
            "default_run".to_string(),
            "1".to_string(),
            "unknown".to_string(),
        )
    })
}
