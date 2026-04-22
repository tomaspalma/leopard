use bincode::serialize;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::sync::Arc;

use message::{impl_protocol_message, MessageType, MessageTypeValues};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MerkleTreeMessageWrapper {
    SyncRoot([u8; 32]),
    SyncNodeRequest(String),
    SyncNodeResponse(String, [u8; 32], [u8; 32], Option<[u8; 32]>, Option<String>),
    DataRequest(String),
    DataResponse(String, String),
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
    wrapper: MerkleTreeMessageWrapper,
}

impl MerkleTreeMessage {
    pub fn new(
        protocol_id: Option<u64>,
        _type: MerkleTreeMessageType,
        wrapper: MerkleTreeMessageWrapper,
    ) -> Self {
        Self {
            protocol_id,
            _type,
            wrapper,
        }
    }

    pub fn wrapper(&self) -> &MerkleTreeMessageWrapper {
        &self.wrapper
    }
}

impl_protocol_message!(MerkleTreeMessage, this, {
    serialize(&this.wrapper).map_err(|_| ())?
});
