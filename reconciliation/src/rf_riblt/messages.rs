use message::{impl_protocol_message, MessageType, MessageTypeValues};
use rkyv::{rancor::Error, Archive, Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub enum RfRibltMessageTypeValues {
    Handshake,
    FilterChunk,
    FilterDone,
    SComSendSymbol,
    SComDecodedAll,
    SComRequestMoreSymbols,
    ValueFetchRequest,
    ValueFetchResponse,
}

impl MessageTypeValues for RfRibltMessageTypeValues {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RfRibltMessageType {
    value: RfRibltMessageTypeValues,
}

impl RfRibltMessageType {
    pub fn new(value: RfRibltMessageTypeValues) -> Self {
        Self { value }
    }
}

impl MessageType for RfRibltMessageType {
    fn value(&self) -> Box<dyn MessageTypeValues> {
        Box::new(self.value.clone())
    }
}

/// Sent once per direction; carries the complete serialized ribbon filter.
#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RfRibltHandshakeMessage {
    _type: RfRibltMessageType,
    protocol_id: Option<u64>,
    pub session_id: String,
    pub ribbon_seed: u64,
    pub filter_bytes: Vec<u8>,
}

impl RfRibltHandshakeMessage {
    pub fn new(
        protocol_id: Option<u64>,
        session_id: String,
        ribbon_seed: u64,
        filter_bytes: Vec<u8>,
    ) -> Self {
        Self {
            _type: RfRibltMessageType::new(RfRibltMessageTypeValues::Handshake),
            protocol_id,
            session_id,
            ribbon_seed,
            filter_bytes,
        }
    }
}

/// One chunk of the serialized ribbon filter bytes.
#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RfRibltFilterChunkMessage {
    _type: RfRibltMessageType,
    protocol_id: Option<u64>,
    pub session_id: String,
    pub chunk_index: u64,
    pub data: Vec<u8>,
}

impl RfRibltFilterChunkMessage {
    pub fn new(
        protocol_id: Option<u64>,
        session_id: String,
        chunk_index: u64,
        data: Vec<u8>,
    ) -> Self {
        Self {
            _type: RfRibltMessageType::new(RfRibltMessageTypeValues::FilterChunk),
            protocol_id,
            session_id,
            chunk_index,
            data,
        }
    }
}

/// Sent after all chunks have been delivered.
#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RfRibltFilterDoneMessage {
    _type: RfRibltMessageType,
    protocol_id: Option<u64>,
    pub session_id: String,
}

impl RfRibltFilterDoneMessage {
    pub fn new(protocol_id: Option<u64>, session_id: String) -> Self {
        Self {
            _type: RfRibltMessageType::new(RfRibltMessageTypeValues::FilterDone),
            protocol_id,
            session_id,
        }
    }
}

pub type RfRibltCodedSymbol = crate::riblt_core::RIBLTCodedSymbol;

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RfRibltSComSendSymbolMessage {
    _type: RfRibltMessageType,
    protocol_id: Option<u64>,
    pub symbols: Vec<RfRibltCodedSymbol>,
    pub session_id: String,
}

impl RfRibltSComSendSymbolMessage {
    pub fn new(
        protocol_id: Option<u64>,
        symbols: Vec<RfRibltCodedSymbol>,
        session_id: String,
    ) -> Self {
        Self {
            _type: RfRibltMessageType::new(RfRibltMessageTypeValues::SComSendSymbol),
            protocol_id,
            symbols,
            session_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RfRibltSComDecodedAllMessage {
    _type: RfRibltMessageType,
    protocol_id: Option<u64>,
    pub session_id: String,
    pub keys_for_sender: Vec<String>,
}

impl RfRibltSComDecodedAllMessage {
    pub fn new(protocol_id: Option<u64>, session_id: String, keys_for_sender: Vec<String>) -> Self {
        Self {
            _type: RfRibltMessageType::new(RfRibltMessageTypeValues::SComDecodedAll),
            protocol_id,
            session_id,
            keys_for_sender,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RfRibltSComRequestMoreSymbolsMessage {
    _type: RfRibltMessageType,
    protocol_id: Option<u64>,
    pub session_id: String,
}

impl RfRibltSComRequestMoreSymbolsMessage {
    pub fn new(protocol_id: Option<u64>, session_id: String) -> Self {
        Self {
            _type: RfRibltMessageType::new(RfRibltMessageTypeValues::SComRequestMoreSymbols),
            protocol_id,
            session_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RfRibltValueFetchRequestMessage {
    _type: RfRibltMessageType,
    protocol_id: Option<u64>,
    pub session_id: String,
    pub keys: Vec<String>,
}

impl RfRibltValueFetchRequestMessage {
    pub fn new(protocol_id: Option<u64>, session_id: String, keys: Vec<String>) -> Self {
        Self {
            _type: RfRibltMessageType::new(RfRibltMessageTypeValues::ValueFetchRequest),
            protocol_id,
            session_id,
            keys,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RfRibltFetchedEntry {
    pub key: String,
    pub value: String,
}

impl RfRibltFetchedEntry {
    pub fn new(key: String, value: String) -> Self {
        Self { key, value }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RfRibltValueFetchResponseMessage {
    _type: RfRibltMessageType,
    protocol_id: Option<u64>,
    pub session_id: String,
    pub entries: Vec<RfRibltFetchedEntry>,
}

impl RfRibltValueFetchResponseMessage {
    pub fn new(
        protocol_id: Option<u64>,
        session_id: String,
        entries: Vec<RfRibltFetchedEntry>,
    ) -> Self {
        Self {
            _type: RfRibltMessageType::new(RfRibltMessageTypeValues::ValueFetchResponse),
            protocol_id,
            session_id,
            entries,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub enum RfRibltMessageWrapper {
    Handshake(RfRibltHandshakeMessage),
    FilterChunk(RfRibltFilterChunkMessage),
    FilterDone(RfRibltFilterDoneMessage),
    SComSendSymbol(RfRibltSComSendSymbolMessage),
    SComDecodedAll(RfRibltSComDecodedAllMessage),
    SComRequestMoreSymbols(RfRibltSComRequestMoreSymbolsMessage),
    ValueFetchRequest(RfRibltValueFetchRequestMessage),
    ValueFetchResponse(RfRibltValueFetchResponseMessage),
}

impl_protocol_message!(RfRibltHandshakeMessage, this, {
    let wrapper = RfRibltMessageWrapper::Handshake(this.clone());
    rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?
});

impl_protocol_message!(RfRibltFilterChunkMessage, this, {
    let wrapper = RfRibltMessageWrapper::FilterChunk(this.clone());
    rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?
});

impl_protocol_message!(RfRibltFilterDoneMessage, this, {
    let wrapper = RfRibltMessageWrapper::FilterDone(this.clone());
    rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?
});

impl_protocol_message!(RfRibltSComSendSymbolMessage, this, {
    let wrapper = RfRibltMessageWrapper::SComSendSymbol(this.clone());
    rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?
});

impl_protocol_message!(RfRibltSComDecodedAllMessage, this, {
    let wrapper = RfRibltMessageWrapper::SComDecodedAll(this.clone());
    rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?
});

impl_protocol_message!(RfRibltSComRequestMoreSymbolsMessage, this, {
    let wrapper = RfRibltMessageWrapper::SComRequestMoreSymbols(this.clone());
    rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?
});

impl_protocol_message!(RfRibltValueFetchRequestMessage, this, {
    let wrapper = RfRibltMessageWrapper::ValueFetchRequest(this.clone());
    rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?
});

impl_protocol_message!(RfRibltValueFetchResponseMessage, this, {
    let wrapper = RfRibltMessageWrapper::ValueFetchResponse(this.clone());
    rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?
});
