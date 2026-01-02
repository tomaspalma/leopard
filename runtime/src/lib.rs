use std::future::Future;
use std::pin::Pin;

use tokio;

pub mod builder;

pub type Task = dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync;

pub trait Runtime {
    fn add_task(&self, task: Box<Task>);
    fn tasks(&self) -> &Vec<Box<Task>>;
}

pub struct TokioRuntime {
    tasks: Vec<Box<Task>>,
}

impl TokioRuntime {
    pub fn new() -> Self {
        Self {
            tasks: vec![],
        }
    }
}

impl Runtime for TokioRuntime {
    fn add_task(&self, task: Box<Task>) {
        tokio::spawn(async move {
            (task)().await.unwrap_or_else(|e| {
                eprintln!("Task failed: {}", e);
            });
        }); 
    }

    fn tasks(&self) -> &Vec<Box<Task>> {
        &self.tasks
    }
}
