use rkyv::{rancor::Error, validation::archive, Archive, Deserialize, Serialize};

use std::rc::Rc;
use std::sync::Arc;

use message::{Message, MessageType, MessageTypeValues};

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub enum RIBLTMessageWrapper {
    SendSymbol(RIBLTSendSymbolMessage),
    DecodedAll(RIBLTDecodedAllMessage),
    RequestMoreSymbols(RIBLTRequestMoreSymbolsMessage),
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub enum RIBLTMessageTypeValues {
    SendSymbol,
    FinishedDecoding,
    RequestMoreSymbols,
}

impl MessageTypeValues for RIBLTMessageTypeValues {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RIBLTMessageType {
    value: RIBLTMessageTypeValues,
}

impl RIBLTMessageType {
    pub fn new(value: RIBLTMessageTypeValues) -> Self {
        Self { value }
    }
}

impl MessageType for RIBLTMessageType {
    fn value(&self) -> Box<dyn MessageTypeValues> {
        Box::new(self.value.clone())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive, PartialEq, Eq, Hash)]
pub struct RIBLTSymbol {
    pub key: String,
    pub value: String,
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
        let mut bytes = Vec::with_capacity(Self::BYTE_ARRAY_LENGTH);
        let key_bytes = self.key.as_bytes();

        // Use 1 byte for key length
        bytes.push(key_bytes.len() as u8);
        bytes.extend_from_slice(key_bytes);

        // Use 1 byte for value length
        let val_bytes = self.value.as_bytes();
        bytes.push(val_bytes.len() as u8);
        bytes.extend_from_slice(val_bytes);

        if bytes.len() < Self::BYTE_ARRAY_LENGTH {
            bytes.resize(Self::BYTE_ARRAY_LENGTH, 0);
        }
        bytes
    }

    fn decode_from_bytes(bytes: &Vec<u8>) -> Self {
        if bytes.is_empty() {
            return Self {
                key: String::new(),
                value: String::new(),
            };
        }

        let key_len = bytes[0] as usize;
        let mut current_idx = 1;

        let key = if current_idx + key_len <= bytes.len() {
            let k = String::from_utf8_lossy(&bytes[current_idx..current_idx + key_len]).to_string();
            current_idx += key_len;
            k
        } else {
            String::new()
        };

        let value_len = if current_idx < bytes.len() {
            let vl = bytes[current_idx] as usize;
            current_idx += 1;
            vl
        } else {
            0
        };

        let value = if current_idx + value_len <= bytes.len() {
            String::from_utf8_lossy(&bytes[current_idx..current_idx + value_len]).to_string()
        } else {
            String::new()
        };

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

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RIBLTDecodedAllMessage {
    _type: RIBLTMessageType,
    protocol_id: Option<u64>,
}

impl RIBLTDecodedAllMessage {
    pub fn new(_type: RIBLTMessageType, protocol_id: Option<u64>) -> Self {
        Self { _type, protocol_id }
    }
}

impl Message for RIBLTDecodedAllMessage {
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

    fn serialize(&self, protocol: Option<u64>, sender_port: u16) -> Result<Vec<u8>, ()> {
        let wrapper = RIBLTMessageWrapper::DecodedAll(self.clone());
        let body_bytes = rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?;

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

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
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

    fn serialize(&self, protocol: Option<u64>, sender_port: u16) -> Result<Vec<u8>, ()> {
        let wrapper = RIBLTMessageWrapper::SendSymbol(self.clone());
        let body_bytes = rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?;

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

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RIBLTRequestMoreSymbolsMessage {
    _type: RIBLTMessageType,
    protocol_id: Option<u64>,
}

impl RIBLTRequestMoreSymbolsMessage {
    pub fn new(_type: RIBLTMessageType, protocol_id: Option<u64>) -> Self {
        Self { _type, protocol_id }
    }
}

impl Message for RIBLTRequestMoreSymbolsMessage {
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

    fn serialize(&self, protocol: Option<u64>, sender_port: u16) -> Result<Vec<u8>, ()> {
        let wrapper = RIBLTMessageWrapper::RequestMoreSymbols(self.clone());
        let body_bytes = rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?;

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
