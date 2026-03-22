pub trait ConnectionInfo<V> {
    fn connection_info(&self) -> V;
}

#[derive(Eq, Hash, PartialEq, Clone)]
pub struct NodePort {
    value: u16,
}

impl NodePort {
    pub fn new(value: u16) -> Self {
        Self { value }
    }

    pub fn value(&self) -> u16 {
        self.value
    }
}

impl ConnectionInfo<u16> for NodePort {
    fn connection_info(&self) -> u16 {
        self.value
    }
}

#[derive(Eq, Hash, PartialEq, Clone, Debug)]
pub struct NodeAddress {
    host: String,
    port: u16,
}

impl NodeAddress {
    pub fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

impl ConnectionInfo<NodeAddress> for NodeAddress {
    fn connection_info(&self) -> NodeAddress {
        self.clone()
    }
}

impl ConnectionInfo<u16> for NodeAddress {
    fn connection_info(&self) -> u16 {
        self.port
    }
}
