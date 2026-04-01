use std::sync::Arc;
use tracing::{error, info};

use connection::{
    node::{id::NodeIdentifier, port::NodeAddress},
    route::RouteTask,
};
use protocol::deserializer::ProtocolDeserializer;
use state::node::DefaultNodeState;

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
                    info!("Root hash mismatch. Requesting node sync.");
                } else {
                    info!("Root hash match. In sync.");
                }
            }
            MerkleTreeMessageWrapper::SyncNodeRequest(node_id) => {
                info!("Received SyncNodeRequest from {:?}", node_id);
            }
            MerkleTreeMessageWrapper::SyncNodeResponse(node_id, root_hash, hash, parent_hash) => {
                info!("Received SyncNodeResponse from {:?}", node_id);
            }
            MerkleTreeMessageWrapper::DataRequest(key) => {
                info!("Received DataRequest for key {:?}", key);
            }
            MerkleTreeMessageWrapper::DataResponse(key, value) => {
                info!("Received DataResponse for key {:?}", key);
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
