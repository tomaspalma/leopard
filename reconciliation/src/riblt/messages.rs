use rkyv::{rancor::Error, validation::archive, Archive, Deserialize, Serialize};

use std::sync::Arc;

use message::{Message, MessageType, MessageTypeValues};

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub enum RIBLTMessageTypeValues {
    SYMBOL,
}

impl MessageTypeValues for RIBLTMessageTypeValues {}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RIBLTMessageType {
    value: RIBLTMessageTypeValues,
}

impl RIBLTMessageType {
    pub fn new() -> Self {
        Self {
            value: RIBLTMessageTypeValues::SYMBOL,
        }
    }
}

impl MessageType for RIBLTMessageType {
    fn value(&self) -> Box<dyn MessageTypeValues> {
        Box::new(RIBLTMessageTypeValues::SYMBOL)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive, PartialEq, Eq, Hash)]
pub struct RIBLTSymbol {
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive, PartialEq, Eq, Hash)]
pub struct RIBLTCodedSymbol {
    pub sum: Vec<u8>,
    pub hash: u64,
    pub count: i64,
}

impl riblt::Symbol for RIBLTSymbol {
    const BYTE_ARRAY_LENGTH: usize = 128;

    fn encode_to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.key.as_bytes().to_vec();
        bytes.extend_from_slice(&self.value);
        if bytes.len() < Self::BYTE_ARRAY_LENGTH {
            bytes.resize(Self::BYTE_ARRAY_LENGTH, 0);
        }
        bytes
    }

    fn decode_from_bytes(bytes: &Vec<u8>) -> Self {
        let key_end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        let key = String::from_utf8_lossy(&bytes[..key_end]).to_string();
        let value = bytes[key_end..].to_vec();
        Self { key, value }
    }
}

impl riblt::Symbol for RIBLTCodedSymbol {
    const BYTE_ARRAY_LENGTH: usize = 128;

    fn encode_to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::BYTE_ARRAY_LENGTH);
        bytes.extend_from_slice(&self.sum);
        bytes.extend_from_slice(&self.hash.to_be_bytes());
        bytes.extend_from_slice(&self.count.to_be_bytes());
        bytes
    }

    fn decode_from_bytes(bytes: &Vec<u8>) -> Self {
        let sum = bytes[0..bytes.len() - 16].to_vec();
        let hash = u64::from_be_bytes(bytes[bytes.len() - 16..bytes.len() - 8].try_into().unwrap());
        let count = i64::from_be_bytes(bytes[bytes.len() - 8..].try_into().unwrap());
        Self { sum, hash, count }
    }
}

#[derive(Debug, Serialize, Deserialize, Archive)]
pub struct RIBLTSendSymbolMessage {
    _type: RIBLTMessageType,
    protocol_id: Option<u64>,
    symbol: Vec<RIBLTCodedSymbol>,
}

impl RIBLTSendSymbolMessage {
    pub fn new(
        _type: RIBLTMessageType,
        protocol_id: Option<u64>,
        symbol: Vec<RIBLTCodedSymbol>,
    ) -> Self {
        Self {
            _type,
            protocol_id,
            symbol,
        }
    }

    pub fn symbols(&self) -> &Vec<RIBLTCodedSymbol> {
        &self.symbol
    }
}

impl Message for RIBLTSendSymbolMessage {
    fn as_any(&self) -> &dyn std::any::Any {
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

    fn serialize(&self, protocol: Option<u64>) -> Result<Vec<u8>, ()> {
        let body_bytes = rkyv::to_bytes::<Error>(self).map_err(|_| ())?;

        let mut packet = Vec::with_capacity(body_bytes.len() + 8);

        if let Some(id) = protocol {
            packet.extend_from_slice(&id.to_be_bytes());
        }

        packet.extend_from_slice(&body_bytes);

        Ok(packet)
    }
}
