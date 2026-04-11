use bincode::deserialize;
use message::Message;
use protocol::deserializer::ProtocolDeserializer;
use std::sync::Arc;

use super::messages::{
    MerkleTreeMessage, MerkleTreeMessageType, MerkleTreeMessageTypeValues, MerkleTreeMessageWrapper,
};

pub struct MerkleTreeDeserializer {}

impl MerkleTreeDeserializer {
    pub fn new() -> Self {
        Self {}
    }
}

impl ProtocolDeserializer for MerkleTreeDeserializer {
    fn deserialize(&self, bytes: Vec<u8>) -> Arc<dyn Message + Send + Sync> {
        if bytes.len() < 16 {
            panic!("Message too short");
        }

        let mut protocol_id_bytes = [0; 8];
        protocol_id_bytes.copy_from_slice(&bytes[0..8]);
        let protocol_id = u64::from_be_bytes(protocol_id_bytes);

        let protocol_id = if protocol_id == 0 {
            None
        } else {
            Some(protocol_id)
        };

        let body_bytes = &bytes[16..];

        let wrapper: MerkleTreeMessageWrapper =
            deserialize(body_bytes).expect("Failed to deserialize MerkleTreeMessage");

        let msg_type_val = match wrapper {
            MerkleTreeMessageWrapper::SyncRoot(_) => MerkleTreeMessageTypeValues::SyncRoot,
            MerkleTreeMessageWrapper::SyncNodeRequest(_) => {
                MerkleTreeMessageTypeValues::SyncNodeRequest
            }
            MerkleTreeMessageWrapper::SyncNodeResponse(_, _, _, _, _) => {
                MerkleTreeMessageTypeValues::SyncNodeResponse
            }
            MerkleTreeMessageWrapper::DataRequest(_) => MerkleTreeMessageTypeValues::DataRequest,
            MerkleTreeMessageWrapper::DataResponse(_, _) => {
                MerkleTreeMessageTypeValues::DataResponse
            }
        };

        let msg_type = MerkleTreeMessageType::new(msg_type_val);

        Arc::new(MerkleTreeMessage::new(protocol_id, msg_type, wrapper))
    }
}
