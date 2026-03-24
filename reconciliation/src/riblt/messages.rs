use rkyv::{rancor::Error, Archive, Deserialize, Serialize};

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

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RIBLTCodedSymbol {
    pub sum: Vec<u8>,
    pub hash: u64,
    pub count: i64,
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
