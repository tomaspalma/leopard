use std::future::Future;
use std::pin::Pin;
use std::sync::{LockResult, PoisonError, RwLock};

use tokio;

pub mod builder;
pub mod time;

pub type Task = dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync;

pub trait Runtime {
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

impl Runtime for TokioRuntime {
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
                    eprintln!("Task failed: {}", e);
                });
            });
        }

        Ok(())
    }
}
