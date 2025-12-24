pub trait NodeConfig {
    pub fn runner() -> Runtime;
}

pub struct DefaultNodeConfig {
    runtime: Runtime
}

impl NodeConfig for DefaultNodeConfig {
    fn runner() -> Runtime {
        
    }
}     