use bincode::serialize;
use serde::{Deserialize, Serialize};
use std::any::Any;

use message::{impl_protocol_message, MessageType, MessageTypeValues};

/// One node's fingerprint in a batched SyncNodeResponse: (node_id, hash, key).
/// `key` is Some only for leaf nodes; a hash of all-zero means the node is
/// absent on the responder's tree.
pub type MerkleNodeAnswer = (String, [u8; 32], Option<String>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MerkleTreeMessageWrapper {
    SyncRoot([u8; 32]),
    // request_id, node-ids whose fingerprints are requested (one tree level).
    SyncNodeRequest(u64, Vec<String>),
    // request_id, fingerprint for each requested node-id.
    SyncNodeResponse(u64, Vec<MerkleNodeAnswer>),
    // request_id, keys whose values are requested.
    DataRequest(u64, Vec<String>),
    // request_id, (key, value) for each available key.
    DataResponse(u64, Vec<(String, String)>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MerkleTreeMessageTypeValues {
    SyncRoot,
    SyncNodeRequest,
    SyncNodeResponse,
    DataRequest,
    DataResponse,
}

impl MessageTypeValues for MerkleTreeMessageTypeValues {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MerkleTreeMessageType {
    value: MerkleTreeMessageTypeValues,
}

impl MerkleTreeMessageType {
    pub fn new(value: MerkleTreeMessageTypeValues) -> Self {
        Self { value }
    }
}

impl MessageType for MerkleTreeMessageType {
    fn value(&self) -> Box<dyn MessageTypeValues> {
        Box::new(self.value.clone())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MerkleTreeMessage {
    protocol_id: Option<u64>,
    _type: MerkleTreeMessageType,
    session_id: String,
    wrapper: MerkleTreeMessageWrapper,
}

impl MerkleTreeMessage {
    pub fn new(
        protocol_id: Option<u64>,
        _type: MerkleTreeMessageType,
        session_id: String,
        wrapper: MerkleTreeMessageWrapper,
    ) -> Self {
        Self {
            protocol_id,
            _type,
            session_id,
            wrapper,
        }
    }

    pub fn wrapper(&self) -> &MerkleTreeMessageWrapper {
        &self.wrapper
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }
}

impl_protocol_message!(MerkleTreeMessage, this, {
    serialize(&(this.session_id.clone(), this.wrapper.clone())).map_err(|_| ())?
});
