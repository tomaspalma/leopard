use message::{impl_protocol_message, MessageType, MessageTypeValues};
use rkyv::{rancor::Error, Archive, Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub enum RbfRibltMessageTypeValues {
    Handshake,
    BloomFilterSlice,
    RBFStopSignal,
    SComSendSymbol,
    SComDecodedAll,
    SComRequestMoreSymbols,
    ValueFetchRequest,
    ValueFetchResponse,
}

impl MessageTypeValues for RbfRibltMessageTypeValues {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RbfRibltMessageType {
    value: RbfRibltMessageTypeValues,
}

impl RbfRibltMessageType {
    pub fn new(value: RbfRibltMessageTypeValues) -> Self {
        Self { value }
    }
}

impl MessageType for RbfRibltMessageType {
    fn value(&self) -> Box<dyn MessageTypeValues> {
        Box::new(self.value.clone())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RbfRibltHandshakeMessage {
    _type: RbfRibltMessageType,
    protocol_id: Option<u64>,
    session_id: String,
}

impl RbfRibltHandshakeMessage {
    pub fn new(_type: RbfRibltMessageType, protocol_id: Option<u64>, session_id: String) -> Self {
        Self {
            _type,
            protocol_id,
            session_id,
        }
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RbfRibltRBFStopSignalMessage {
    _type: RbfRibltMessageType,
    protocol_id: Option<u64>,
    session_id: String,
}

impl RbfRibltRBFStopSignalMessage {
    pub fn new(protocol_id: Option<u64>, session_id: String) -> Self {
        Self {
            _type: RbfRibltMessageType::new(RbfRibltMessageTypeValues::RBFStopSignal),
            protocol_id,
            session_id,
        }
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RbfRibltBloomFilterSliceMessage {
    _type: RbfRibltMessageType,
    protocol_id: Option<u64>,
    session_id: String,
    slice_index: u64,
    m: usize,
    k: u64,
    seeds: [u64; 2],
    bits: Vec<u8>,
}

impl RbfRibltBloomFilterSliceMessage {
    pub fn new(
        protocol_id: Option<u64>,
        session_id: String,
        slice_index: u64,
        m: usize,
        k: u64,
        seeds: [u64; 2],
        bits: Vec<u8>,
    ) -> Self {
        Self {
            _type: RbfRibltMessageType::new(RbfRibltMessageTypeValues::BloomFilterSlice),
            protocol_id,
            session_id,
            slice_index,
            m,
            k,
            seeds,
            bits,
        }
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn slice_index(&self) -> u64 {
        self.slice_index
    }

    pub fn m(&self) -> usize {
        self.m
    }

    pub fn k(&self) -> u64 {
        self.k
    }

    pub fn seeds(&self) -> [u64; 2] {
        self.seeds
    }

    pub fn bits(&self) -> &Vec<u8> {
        &self.bits
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive, PartialEq, Eq, Hash)]
pub struct RbfRibltCodedSymbol {
    pub sum: Vec<u8>,
    pub hash: u64,
    pub count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RbfRibltSComSendSymbolMessage {
    _type: RbfRibltMessageType,
    protocol_id: Option<u64>,
    symbols: Vec<RbfRibltCodedSymbol>,
    session_id: String,
}

impl RbfRibltSComSendSymbolMessage {
    pub fn new(
        protocol_id: Option<u64>,
        symbols: Vec<RbfRibltCodedSymbol>,
        session_id: String,
    ) -> Self {
        Self {
            _type: RbfRibltMessageType::new(RbfRibltMessageTypeValues::SComSendSymbol),
            protocol_id,
            symbols,
            session_id,
        }
    }

    pub fn symbols(&self) -> &Vec<RbfRibltCodedSymbol> {
        &self.symbols
    }

    pub fn session_id(&self) -> &String {
        &self.session_id
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RbfRibltSComDecodedAllMessage {
    _type: RbfRibltMessageType,
    protocol_id: Option<u64>,
    session_id: String,
    /// Keys the IBLT-sender node should fetch from the IBLT-receiver.
    /// Computed by the receiver as: (IBLT-local peeled keys) + (receiver's s_tn for this neighbor).
    keys_for_sender: Vec<String>,
}

impl RbfRibltSComDecodedAllMessage {
    pub fn new(protocol_id: Option<u64>, session_id: String, keys_for_sender: Vec<String>) -> Self {
        Self {
            _type: RbfRibltMessageType::new(RbfRibltMessageTypeValues::SComDecodedAll),
            protocol_id,
            session_id,
            keys_for_sender,
        }
    }

    pub fn session_id(&self) -> &String {
        &self.session_id
    }

    pub fn keys_for_sender(&self) -> &Vec<String> {
        &self.keys_for_sender
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RbfRibltSComRequestMoreSymbolsMessage {
    _type: RbfRibltMessageType,
    protocol_id: Option<u64>,
    session_id: String,
}

impl RbfRibltSComRequestMoreSymbolsMessage {
    pub fn new(protocol_id: Option<u64>, session_id: String) -> Self {
        Self {
            _type: RbfRibltMessageType::new(RbfRibltMessageTypeValues::SComRequestMoreSymbols),
            protocol_id,
            session_id,
        }
    }

    pub fn session_id(&self) -> &String {
        &self.session_id
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RbfRibltValueFetchRequestMessage {
    _type: RbfRibltMessageType,
    protocol_id: Option<u64>,
    session_id: String,
    keys: Vec<String>,
}

impl RbfRibltValueFetchRequestMessage {
    pub fn new(protocol_id: Option<u64>, session_id: String, keys: Vec<String>) -> Self {
        Self {
            _type: RbfRibltMessageType::new(RbfRibltMessageTypeValues::ValueFetchRequest),
            protocol_id,
            session_id,
            keys,
        }
    }

    pub fn session_id(&self) -> &String {
        &self.session_id
    }

    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RbfRibltFetchedEntry {
    key: String,
    value: String,
}

impl RbfRibltFetchedEntry {
    pub fn new(key: String, value: String) -> Self {
        Self { key, value }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct RbfRibltValueFetchResponseMessage {
    _type: RbfRibltMessageType,
    protocol_id: Option<u64>,
    session_id: String,
    entries: Vec<RbfRibltFetchedEntry>,
}

impl RbfRibltValueFetchResponseMessage {
    pub fn new(
        protocol_id: Option<u64>,
        session_id: String,
        entries: Vec<RbfRibltFetchedEntry>,
    ) -> Self {
        Self {
            _type: RbfRibltMessageType::new(RbfRibltMessageTypeValues::ValueFetchResponse),
            protocol_id,
            session_id,
            entries,
        }
    }

    pub fn session_id(&self) -> &String {
        &self.session_id
    }

    pub fn entries(&self) -> &Vec<RbfRibltFetchedEntry> {
        &self.entries
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub enum RbfRibltMessageWrapper {
    Handshake(RbfRibltHandshakeMessage),
    BloomFilterSlice(RbfRibltBloomFilterSliceMessage),
    RBFStopSignal(RbfRibltRBFStopSignalMessage),
    SComSendSymbol(RbfRibltSComSendSymbolMessage),
    SComDecodedAll(RbfRibltSComDecodedAllMessage),
    SComRequestMoreSymbols(RbfRibltSComRequestMoreSymbolsMessage),
    ValueFetchRequest(RbfRibltValueFetchRequestMessage),
    ValueFetchResponse(RbfRibltValueFetchResponseMessage),
}

impl_protocol_message!(RbfRibltHandshakeMessage, this, {
    let wrapper = RbfRibltMessageWrapper::Handshake(this.clone());
    rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?
});

impl_protocol_message!(RbfRibltRBFStopSignalMessage, this, {
    let wrapper = RbfRibltMessageWrapper::RBFStopSignal(this.clone());
    rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?
});

impl_protocol_message!(RbfRibltBloomFilterSliceMessage, this, {
    let wrapper = RbfRibltMessageWrapper::BloomFilterSlice(this.clone());
    rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?
});

impl_protocol_message!(RbfRibltSComSendSymbolMessage, this, {
    let wrapper = RbfRibltMessageWrapper::SComSendSymbol(this.clone());
    rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?
});

impl_protocol_message!(RbfRibltSComDecodedAllMessage, this, {
    let wrapper = RbfRibltMessageWrapper::SComDecodedAll(this.clone());
    rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?
});

impl_protocol_message!(RbfRibltSComRequestMoreSymbolsMessage, this, {
    let wrapper = RbfRibltMessageWrapper::SComRequestMoreSymbols(this.clone());
    rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?
});

impl_protocol_message!(RbfRibltValueFetchRequestMessage, this, {
    let wrapper = RbfRibltMessageWrapper::ValueFetchRequest(this.clone());
    rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?
});

impl_protocol_message!(RbfRibltValueFetchResponseMessage, this, {
    let wrapper = RbfRibltMessageWrapper::ValueFetchResponse(this.clone());
    rkyv::to_bytes::<Error>(&wrapper).map_err(|_| ())?
});
