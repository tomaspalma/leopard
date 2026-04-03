use std::sync::Arc;
use tracing::{error, info};

use connection::{
    node::{id::NodeIdentifier, port::NodeAddress},
    route::RouteTask,
};
use message::Message;
use protocol::deserializer::ProtocolDeserializer;
use runtime::spawn;
use state::node::{DefaultNodeState, NodeState};

use crate::merkle_tree::messages::{MerkleTreeMessageType, MerkleTreeMessageTypeValues};

use super::{
    deserializer::MerkleTreeDeserializer,
    messages::{MerkleTreeMessage, MerkleTreeMessageWrapper},
    tree::BinaryMerkleTree,
};

pub struct ReceiveMerkleTreeMessageTask {
    identifier: Arc<dyn NodeIdentifier<NodeAddress, NodeAddress> + Send + Sync>,
    state: Arc<DefaultNodeState>,
    tree: Arc<BinaryMerkleTree>,
}

impl ReceiveMerkleTreeMessageTask {
    pub fn new(
        identifier: Arc<dyn NodeIdentifier<NodeAddress, NodeAddress> + Send + Sync>,
        state: Arc<DefaultNodeState>,
        tree: Arc<BinaryMerkleTree>,
    ) -> Self {
        Self {
            identifier,
            state,
            tree,
        }
    }

    fn handle_message(&self, msg: &MerkleTreeMessage, neighbor: NodeAddress) {
        match msg.wrapper() {
            MerkleTreeMessageWrapper::SyncRoot(root_hash) => {
                let local_root = self.tree.get_root_hash();
                if local_root != *root_hash {
                    info!("Root hash mismatch. Requesting children nodes sync.");

                    let state_clone = self.state.clone();
                    let id_clone = self.identifier.connection_info().clone();
                    let neighbor_clone = neighbor.clone();
                    let protocol_id = msg.protocol();

                    spawn!({
                        let request_left = MerkleTreeMessage::new(
                            protocol_id,
                            MerkleTreeMessageType::new(
                                MerkleTreeMessageTypeValues::SyncNodeRequest,
                            ),
                            MerkleTreeMessageWrapper::SyncNodeRequest("root-left".to_string()),
                        );
                        let _ = state_clone
                            .send_through_socket(
                                id_clone.clone(),
                                Box::new(neighbor_clone.clone()),
                                Box::new(request_left),
                            )
                            .await;

                        let request_right = MerkleTreeMessage::new(
                            protocol_id,
                            MerkleTreeMessageType::new(
                                MerkleTreeMessageTypeValues::SyncNodeRequest,
                            ),
                            MerkleTreeMessageWrapper::SyncNodeRequest("root-right".to_string()),
                        );
                        let _ = state_clone
                            .send_through_socket(
                                id_clone,
                                Box::new(neighbor_clone),
                                Box::new(request_right),
                            )
                            .await;
                    });
                } else {
                    info!("Root hash match. In sync.");
                }
            }
            MerkleTreeMessageWrapper::SyncNodeRequest(node_id) => {
                info!("Received SyncNodeRequest from {:?}", node_id);
                let root_hash = self.tree.get_root_hash();

                let (node_hash, parent_hash, node_key) =
                    if let Some((node, p_hash)) = self.tree.get_node(node_id) {
                        (node.hash, p_hash, node.key)
                    } else {
                        ([0; 32], None, None)
                    };

                let state_clone = self.state.clone();
                let id_clone = self.identifier.connection_info().clone();
                let neighbor_clone = neighbor.clone();
                let protocol_id = msg.protocol();
                let node_id_clone = node_id.clone();

                spawn!({
                    let response = MerkleTreeMessage::new(
                        protocol_id,
                        MerkleTreeMessageType::new(MerkleTreeMessageTypeValues::SyncNodeResponse),
                        MerkleTreeMessageWrapper::SyncNodeResponse(
                            node_id_clone,
                            root_hash,
                            node_hash,
                            parent_hash,
                            node_key,
                        ),
                    );
                    let _ = state_clone
                        .send_through_socket(id_clone, Box::new(neighbor_clone), Box::new(response))
                        .await;
                });
            }
            MerkleTreeMessageWrapper::SyncNodeResponse(
                node_id,
                _root_hash,
                hash,
                _parent_hash,
                remote_key,
            ) => {
                info!("Received SyncNodeResponse for {:?}", node_id);
                let local_node = self.tree.get_node(node_id);
                let local_hash = local_node.map_or([0; 32], |(n, _)| n.hash);

                if local_hash != *hash {
                    let state_clone = self.state.clone();
                    let id_clone = self.identifier.connection_info().clone();
                    let neighbor_clone = neighbor.clone();
                    let protocol_id = msg.protocol();
                    let node_id_clone = node_id.clone();

                    if let Some(key) = remote_key {
                        info!("Leaf node mismatch. Requesting data for key: {:?}", key);
                        let key_clone = key.clone();
                        spawn!({
                            let request = MerkleTreeMessage::new(
                                protocol_id,
                                MerkleTreeMessageType::new(
                                    MerkleTreeMessageTypeValues::DataRequest,
                                ),
                                MerkleTreeMessageWrapper::DataRequest(key_clone),
                            );
                            let _ = state_clone
                                .send_through_socket(
                                    id_clone,
                                    Box::new(neighbor_clone),
                                    Box::new(request),
                                )
                                .await;
                        });
                    } else if *hash != [0; 32] {
                        info!(
                            "Internal node mismatch at {:?}. Requesting children.",
                            node_id
                        );
                        spawn!({
                            let request_left = MerkleTreeMessage::new(
                                protocol_id,
                                MerkleTreeMessageType::new(
                                    MerkleTreeMessageTypeValues::SyncNodeRequest,
                                ),
                                MerkleTreeMessageWrapper::SyncNodeRequest(format!(
                                    "{}-left",
                                    node_id_clone
                                )),
                            );
                            let _ = state_clone
                                .send_through_socket(
                                    id_clone.clone(),
                                    Box::new(neighbor_clone.clone()),
                                    Box::new(request_left),
                                )
                                .await;

                            let request_right = MerkleTreeMessage::new(
                                protocol_id,
                                MerkleTreeMessageType::new(
                                    MerkleTreeMessageTypeValues::SyncNodeRequest,
                                ),
                                MerkleTreeMessageWrapper::SyncNodeRequest(format!(
                                    "{}-right",
                                    node_id_clone
                                )),
                            );
                            let _ = state_clone
                                .send_through_socket(
                                    id_clone,
                                    Box::new(neighbor_clone),
                                    Box::new(request_right),
                                )
                                .await;
                        });
                    }
                }
            }
            MerkleTreeMessageWrapper::DataRequest(key) => {
                info!("Received DataRequest for key {:?}", key);
                if let Some(value) = self.tree.data.read().unwrap().get(key).cloned() {
                    let state_clone = self.state.clone();
                    let id_clone = self.identifier.connection_info().clone();
                    let neighbor_clone = neighbor.clone();
                    let protocol_id = msg.protocol();
                    let key_clone = key.clone();

                    spawn!({
                        let response = MerkleTreeMessage::new(
                            protocol_id,
                            MerkleTreeMessageType::new(MerkleTreeMessageTypeValues::DataResponse),
                            MerkleTreeMessageWrapper::DataResponse(key_clone, value),
                        );
                        let _ = state_clone
                            .send_through_socket(
                                id_clone,
                                Box::new(neighbor_clone),
                                Box::new(response),
                            )
                            .await;
                    });
                }
            }
            MerkleTreeMessageWrapper::DataResponse(key, value) => {
                info!(
                    "Received DataResponse for key {:?}. Inserting into local tree.",
                    key
                );
                self.tree.insert(key.clone(), value.clone());
            }
            _ => info!("Received unknwon message type from {:?}", neighbor),
        }
    }
}

impl RouteTask for ReceiveMerkleTreeMessageTask {
    fn run(&self, message: Vec<u8>, neighbor: NodeAddress) {
        let deserialized_message = MerkleTreeDeserializer::new().deserialize(message);

        if let Some(msg) = deserialized_message
            .as_any()
            .downcast_ref::<MerkleTreeMessage>()
        {
            self.handle_message(msg, neighbor);
        } else {
            error!("Failed to downcast MerkleTreeMessage");
        }
    }
}
