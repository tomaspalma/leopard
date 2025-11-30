use tokio;

pub type Task = dyn Fn() -> Result<(), Box<dyn std::error::Error>> + Sync + Send;

trait Runtime {
    fn new(main: Box<Task>) -> Self;
    fn init(&mut self);
    fn is_init(&self) -> bool;
    fn add_task(&self, task: Box<Task>);
}

pub struct TokioRuntime {
    init: bool,
    main: Box<Task>,
}

impl Runtime for TokioRuntime {
    fn new(main: Box<Task>) -> Self {
        Self {
            main,
            init: false,
        } 
    }

    fn init(&mut self) {
        match tokio::runtime::Runtime::new() {
            Ok(rt) => {
                rt.block_on(async {
                    self.init = true;

                    (self.main)().unwrap_or_else(|_| {
                        self.init = false;
                    });
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
            (task)().unwrap_or_else(|e| {
                eprintln!("Task failed: {}", e);
            });
        }); 
    }

    fn is_init(&self) -> bool {
        self.init
    }
}
