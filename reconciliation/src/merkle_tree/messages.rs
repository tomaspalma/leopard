use bincode::serialize;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::sync::Arc;

use message::{Message, MessageType, MessageTypeValues};

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

impl Message for MerkleTreeMessage {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_type(&self) -> Arc<dyn MessageType + Send + Sync> {
        Arc::new(self._type.clone())
    }

    fn content(&self) -> Arc<Vec<u8>> {
        Arc::new(vec![])
    }

    fn protocol(&self) -> Option<u64> {
        self.protocol_id
    }

    fn serialize(&self, protocol: Option<u64>, sender_port: u16) -> Result<Vec<u8>, ()> {
        let body_bytes = serialize(&self.wrapper).map_err(|_| ())?;

        let mut packet = Vec::with_capacity(body_bytes.len() + 16);

        if let Some(id) = protocol {
            packet.extend_from_slice(&id.to_be_bytes());
        } else {
            packet.extend_from_slice(&[0; 8]);
        }

        packet.extend_from_slice(&sender_port.to_be_bytes());
        packet.extend_from_slice(&[0; 6]);
        packet.extend_from_slice(&body_bytes);

        Ok(packet)
    }
}
