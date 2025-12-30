use std::future::Future;
use std::pin::Pin;

use tokio;

pub mod builder;
pub mod runner;

pub type Task = dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync;

pub trait Runtime {
    fn init(&mut self);
    fn is_init(&self) -> bool;
    fn add_task(&self, task: Box<Task>);
}

#[derive(Default)]
pub struct TokioRuntime {
    init: bool,
    main: Option<Box<Task>>,
}

impl TokioRuntime {
    pub fn new(main: Option<Box<Task>>) -> Self {
        Self {
            main,
            init: false
        }
    }
}

impl Runtime for TokioRuntime {
    fn init(&mut self) {
        match tokio::runtime::Runtime::new() {
            Ok(rt) => {
                rt.block_on(async {
                    self.init = true;

                    if let Some(main) = &self.main {
                        (main)().await.unwrap_or_else(|e| {
                            eprintln!("Main task failed: {}", e);
                            self.init = false;
                        });
                    }
                });
            }
            Err(e) => {
                panic!("Failed to create Tokio runtime: {}", e);
            }
        }
    }

    fn add_task(&self, task: Box<Task>) {
        if !self.init {
            panic!("Runtime not initialized");
        }

        tokio::spawn(async move {
            (task)().await.unwrap_or_else(|e| {
                eprintln!("Task failed: {}", e);
            });
        }); 
    }

    fn is_init(&self) -> bool {
        self.init
    }
}
