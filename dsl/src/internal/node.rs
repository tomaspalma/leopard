use std::sync::Arc;
use std::{future::Future, pin::Pin};

use config::node::DefaultNodeConfig;
use connection::node::id::DefaultNodeIdentifier;
use connection::node::port::NodeAddress;
use connection::route::default::DefaultRouteHandler;
use membership_protocols::DefaultMembershipProtocol;
use node::Node;
use reconciliation::merkle_tree::protocol::MerkleTreeReconciliationProtocol;
use reconciliation::rbf_riblt::RbfRibltProtocol;
use reconciliation::rf_riblt::RfRibltProtocol;
use reconciliation::riblt::RIBLT;
use replication::protocol::EagerBroadcastReplicationProtocol;
use runtime::Task;
use services::http::NodeHTTPService;
use state::checker::ReconciliationChecker;
use state::node::{DefaultNodeState, NodeState};
use state::storage::state::{DataState, DefaultDataState};
use tracing::{Instrument, info};

use super::checker::{CheckerBuilder, build_checker};
use super::service::{ServiceBuilder, ServiceConfig};

#[derive(Clone)]
pub enum ProtocolChoice {
    Merkle,
    Riblt,
    RbfRiblt,
    RfRiblt,
    Replication,
}

pub struct BuildResult {
    pub tasks: Vec<Box<Task>>,
    pub checker: Arc<dyn ReconciliationChecker>,
}

struct InternalNode {
    host: String,
    port: Option<u16>,
    dataset: Option<String>,
    protocols: Vec<ProtocolChoice>,
    services: Vec<ServiceBuilder>,
}

impl InternalNode {
    fn new() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: None,
            dataset: None,
            protocols: vec![],
            services: vec![],
        }
    }
}

pub struct NodeBuilder {
    nodes: Vec<InternalNode>,
    checker: Option<CheckerBuilder>,
}

impl Default for NodeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeBuilder {
    pub fn new() -> Self {
        Self {
            nodes: vec![],
            checker: None,
        }
    }

    pub fn node(mut self) -> Self {
        self.nodes.push(InternalNode::new());
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        if self.nodes.is_empty() {
            self.nodes.push(InternalNode::new());
        }
        if let Some(node) = self.nodes.last_mut() {
            node.port = Some(port);
        }
        self
    }

    pub fn addr(mut self, addr: impl Into<String>) -> Self {
        if self.nodes.is_empty() {
            self.nodes.push(InternalNode::new());
        }
        if let Some(node) = self.nodes.last_mut() {
            node.host = addr.into();
        }
        self
    }

    pub fn dataset(mut self, path: impl Into<String>) -> Self {
        if self.nodes.is_empty() {
            self.nodes.push(InternalNode::new());
        }
        if let Some(node) = self.nodes.last_mut() {
            node.dataset = Some(path.into());
        }
        self
    }

    pub fn protocol(mut self, protocol: ProtocolChoice) -> Self {
        if self.nodes.is_empty() {
            self.nodes.push(InternalNode::new());
        }
        if let Some(node) = self.nodes.last_mut() {
            node.protocols.push(protocol);
        }
        self
    }

    pub fn service(mut self, service: ServiceBuilder) -> Self {
        if self.nodes.is_empty() {
            self.nodes.push(InternalNode::new());
        }
        if let Some(node) = self.nodes.last_mut() {
            node.services.push(service);
        }
        self
    }

    pub fn checker(mut self, checker: CheckerBuilder) -> Self {
        self.checker = Some(checker);
        self
    }

    fn create_node_task(
        host: String,
        port: u16,
        protocols: Vec<ProtocolChoice>,
        storage: Arc<dyn DataState + Send + Sync>,
        checker: Arc<dyn ReconciliationChecker>,
        services: Vec<ServiceConfig>,
    ) -> Box<Task> {
        Box::new(move || {
            let host = host.clone();
            let protocols = protocols.clone();
            let storage = storage.clone();
            let checker = checker.clone();
            let services = services.clone();

            let future: Pin<Box<dyn Future<Output = Result<(), String>> + Send>> =
                Box::pin(async move {
                    let node_name = format!("{}:{}", host, port);
                    let span = tracing::info_span!("node", name = %node_name);

                    async move {
                        info!("Starting node at {}:{}", host, port);

                        let address = NodeAddress::new(host.clone(), port);
                        let config = Arc::new(DefaultNodeConfig::new());
                        let node_id = Arc::new(DefaultNodeIdentifier::new(
                            node_name.clone(),
                            address.clone(),
                        ));

                        let node_state = Arc::new(DefaultNodeState::new(
                            config.clone(),
                            node_id,
                            Arc::new(DefaultRouteHandler::new()),
                        ));

                        node_state.register_storage("default".to_string(), storage);
                        node_state.set_reconciliation_checker(checker);

                        let mut node = Node::new(
                            node_state.clone(),
                            config.clone(),
                            Box::new(DefaultNodeIdentifier::new(
                                node_name.clone(),
                                address.clone(),
                            )),
                        );

                        node.add_protocol(Box::new(DefaultMembershipProtocol::new()));

                        for protocol in protocols {
                            match protocol {
                                ProtocolChoice::Merkle => node.add_protocol(Box::new(
                                    MerkleTreeReconciliationProtocol::new(
                                        node_state.clone(),
                                        address.clone(),
                                    ),
                                )),
                                ProtocolChoice::Riblt => node.add_protocol(Box::new(RIBLT::new(
                                    node_state.clone(),
                                    address.clone(),
                                ))),
                                ProtocolChoice::RbfRiblt => node.add_protocol(Box::new(
                                    RbfRibltProtocol::new(node_state.clone(), address.clone()),
                                )),
                                ProtocolChoice::RfRiblt => node.add_protocol(Box::new(
                                    RfRibltProtocol::new(node_state.clone(), address.clone()),
                                )),
                                ProtocolChoice::Replication => node.add_protocol(Box::new(
                                    EagerBroadcastReplicationProtocol::new(
                                        node_state.clone(),
                                        address.clone(),
                                    ),
                                )),
                            }
                        }

                        for service in services {
                            match service {
                                ServiceConfig::Http { port: http_port } => {
                                    node.add_service(Arc::new(NodeHTTPService::new(
                                        NodeAddress::new(host.clone(), http_port),
                                        node_state.clone(),
                                    )));
                                }
                                ServiceConfig::Ws { .. } => {
                                    unimplemented!("WebSocket service not yet supported")
                                }
                            }
                        }

                        node.init().await.map_err(|e| e.to_string())
                    }
                    .instrument(span)
                    .await
                });

            future
        })
    }

    pub async fn build(self) -> Result<BuildResult, String> {
        if self.nodes.is_empty() {
            return Err("no nodes configured in NodeBuilder".to_string());
        }

        let checker_choice = self
            .checker
            .ok_or("no checker configured in NodeBuilder")?
            .build()?;

        let storages: Vec<Arc<dyn DataState + Send + Sync>> =
            futures::future::join_all(self.nodes.iter().map(|node| {
                let dataset = node.dataset.clone().unwrap_or_default();
                async move {
                    Arc::new(DefaultDataState::new(dataset).await)
                        as Arc<dyn DataState + Send + Sync>
                }
            }))
            .await;

        let checker = build_checker(&checker_choice, storages.clone());

        let mut tasks = Vec::with_capacity(self.nodes.len());

        for (node, storage) in self.nodes.into_iter().zip(storages) {
            let port = node.port.ok_or("missing port on node")?;
            if node.protocols.is_empty() {
                return Err("missing protocol on node".to_string());
            }

            let services = node
                .services
                .into_iter()
                .map(ServiceBuilder::build)
                .collect::<Result<Vec<ServiceConfig>, String>>()?;

            tasks.push(Self::create_node_task(
                node.host,
                port,
                node.protocols,
                storage,
                checker.clone(),
                services,
            ));
        }

        Ok(BuildResult { tasks, checker })
    }
}
