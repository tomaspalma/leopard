use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, LockResult, PoisonError, RwLock};
use tracing::error;

pub use tokio;

pub mod macros;
pub mod metrics;
pub mod time;

pub static RUNTIME: LazyLock<RwLock<Arc<dyn Runtime + Send + Sync>>> =
    LazyLock::new(|| RwLock::new(Arc::new(TokioRuntime::new())));

pub type Task = dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync;

#[async_trait]
pub trait Runtime {
    fn spawn(&self, task: Pin<Box<dyn Future<Output = ()> + Send>>);
    fn add_task(&self, task: Box<Task>) -> LockResult<()>;
    fn tasks(&self) -> &RwLock<Vec<Box<Task>>>;
    fn init(&self) -> Result<(), String>;
}

pub struct TokioRuntime {
    tasks: RwLock<Vec<Box<Task>>>,
}

impl TokioRuntime {
    pub fn new() -> Self {
        Self {
            tasks: RwLock::new(vec![]),
        }
    }
}

pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    let guard = RUNTIME.read().unwrap();
    guard.spawn(Box::pin(future));
}

/// Whether storage backends should persist to disk. Benchmarks set
/// `DISABLE_STORAGE_FLUSH=1` (or `true`) to keep disk I/O off the measured
/// path, so the collected metrics reflect the protocol rather than persistence.
pub fn storage_flush_enabled() -> bool {
    !matches!(
        std::env::var("DISABLE_STORAGE_FLUSH").as_deref(),
        Ok("1") | Ok("true")
    )
}

impl Runtime for TokioRuntime {
    fn spawn(&self, task: Pin<Box<dyn Future<Output = ()> + Send>>) {
        tokio::spawn(task);
    }

    fn add_task(&self, task: Box<Task>) -> LockResult<()> {
        match self.tasks.write() {
            Ok(mut tasks) => {
                tasks.push(task);

                Ok(())
            }
            Err(_) => Err(PoisonError::new(())),
        }
    }

    fn tasks(&self) -> &RwLock<Vec<Box<Task>>> {
        &self.tasks
    }

    fn init(&self) -> Result<(), String> {
        for task in self.tasks.read().unwrap().iter() {
            let fut = (task)();
            tokio::spawn(async move {
                fut.await.unwrap_or_else(|e| {
                    error!("Task failed: {}", e);
                });
            });
        }

        Ok(())
    }
}
