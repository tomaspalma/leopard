use rkyv::{rancor::Error, Archive, Deserialize, Serialize};


use message::{impl_protocol_message, MessageType, MessageTypeValues};

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
    session_id: String,
}

impl RIBLTDecodedAllMessage {
    pub fn new(_type: RIBLTMessageType, protocol_id: Option<u64>, session_id: String) -> Self {
        Self {
            _type,
            protocol_id,
            session_id,
        }
    }

    pub fn session_id(&self) -> &String {
        &self.session_id
    }
}

impl_protocol_message!(RIBLTDecodedAllMessage, this, {
    let wrapper = RIBLTMessageWrapper::DecodedAll(this.clone());
    rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?
});

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RIBLTSendSymbolMessage {
    _type: RIBLTMessageType,
    protocol_id: Option<u64>,
    symbol: Vec<RIBLTCodedSymbol>,
    session_id: String,
    // Encoder index of the first symbol in this batch. The decoder is positional
    // (the k-th coded symbol added must be encoder index k), but batches travel
    // on independent connections and can arrive out of order, so the receiver
    // uses this to reassemble the stream in order before decoding.
    start_index: u64,
}

impl RIBLTSendSymbolMessage {
    pub fn new(
        _type: RIBLTMessageType,
        protocol_id: Option<u64>,
        symbol: Vec<RIBLTCodedSymbol>,
        session_id: String,
        start_index: u64,
    ) -> Self {
        Self {
            _type,
            protocol_id,
            symbol,
            session_id,
            start_index,
        }
    }

    pub fn symbols(&self) -> &Vec<RIBLTCodedSymbol> {
        &self.symbol
    }

    pub fn session_id(&self) -> &String {
        &self.session_id
    }

    pub fn start_index(&self) -> u64 {
        self.start_index
    }
}

impl_protocol_message!(RIBLTSendSymbolMessage, this, {
    let wrapper = RIBLTMessageWrapper::SendSymbol(this.clone());
    rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?
});

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RIBLTRequestMoreSymbolsMessage {
    _type: RIBLTMessageType,
    protocol_id: Option<u64>,
    session_id: String,
    // Total number of coded symbols the receiver has consumed so far. Doubles as
    // a flow-control credit: the sender may keep up to WINDOW symbols in flight
    // beyond this acknowledged count.
    received_count: u64,
}

impl RIBLTRequestMoreSymbolsMessage {
    pub fn new(
        _type: RIBLTMessageType,
        protocol_id: Option<u64>,
        session_id: String,
        received_count: u64,
    ) -> Self {
        Self {
            _type,
            protocol_id,
            session_id,
            received_count,
        }
    }

    pub fn session_id(&self) -> &String {
        &self.session_id
    }

    pub fn received_count(&self) -> u64 {
        self.received_count
    }
}

impl_protocol_message!(RIBLTRequestMoreSymbolsMessage, this, {
    let wrapper = RIBLTMessageWrapper::RequestMoreSymbols(this.clone());
    rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?
});
