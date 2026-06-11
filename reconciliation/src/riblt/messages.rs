use rkyv::{rancor::Error, Archive, Deserialize, Serialize};


use message::{impl_protocol_message, MessageType, MessageTypeValues};

use crate::riblt_core::RIBLTCodedSymbol;

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
