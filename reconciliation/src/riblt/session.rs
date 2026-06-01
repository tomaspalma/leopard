use std::collections::HashSet;

use riblt::{CodedSymbol, Decoder};
use state::node::{DefaultNodeState, NodeState};
use state::storage::item::DefaultDataStateItem;

use crate::riblt::messages::{RIBLTCodedSymbol, RIBLTSymbol};

pub struct IbltPeelResult {
    pub successful: bool,
    // Only the remote symbols decoded since `remote_cursor`, not the whole
    // accumulated set, so each batch stores O(new) instead of O(total).
    pub remote_symbols: Vec<RIBLTSymbol>,
    // Total number of remote symbols decoded so far; the caller saves this back
    // as the next cursor.
    pub remote_total: usize,
    // Full local-only set, collected only once peeling succeeds (consumers only
    // read it on success). Empty until then.
    pub local_symbols: Vec<RIBLTSymbol>,
}

/// Build the local symbol set from all items in the node's default storage.
pub fn load_iblt_symbols(state: &DefaultNodeState) -> HashSet<RIBLTSymbol> {
    let mut symbols = HashSet::new();
    if let Some(storage) = state.get_storage("default".to_string()) {
        for item in storage.items() {
            symbols.insert(RIBLTSymbol {
                key: item.key().to_string(),
                value: item.value().to_string(),
            });
        }
    }
    symbols
}

/// Write a batch of reconciled symbols back to the node's default storage.
pub async fn store_symbols(state: &DefaultNodeState, symbols: Vec<RIBLTSymbol>) {
    if let Some(storage) = state.get_storage("default".to_string()) {
        for symbol in symbols {
            storage
                .store(Box::new(DefaultDataStateItem::new(symbol.key, symbol.value)))
                .await;
        }
    }
}

/// Feed a batch of wire-format coded symbols into a Decoder.
pub fn add_coded_symbols(decoder: &mut Decoder<RIBLTSymbol>, symbols: &[RIBLTCodedSymbol]) {
    for symbol in symbols {
        let cs = CodedSymbol::from_parts(symbol.sum.clone(), symbol.hash, symbol.count);
        decoder.add_coded_symbol(cs);
    }
}

/// Collect the peel result from a decoder after `try_decode` has run.
fn peel_result(decoder: &Decoder<RIBLTSymbol>, remote_cursor: usize) -> IbltPeelResult {
    let successful = decoder.decoded();
    let all_remote = decoder.remote_symbols();
    let remote_total = all_remote.len();
    let remote_symbols = all_remote[remote_cursor.min(remote_total)..]
        .iter()
        .map(|hs| hs.symbol.clone())
        .collect();
    let local_symbols = if successful {
        decoder.local_symbols().iter().map(|hs| hs.symbol.clone()).collect()
    } else {
        Vec::new()
    };
    IbltPeelResult { successful, remote_symbols, remote_total, local_symbols }
}

/// Run `try_decode` on a blocking thread and return the decoder alongside the
/// peel results.  The decoder is moved in and out so the caller can store it
/// back into the receiving state after the await.
pub async fn try_decode_blocking(
    decoder: Decoder<RIBLTSymbol>,
    remote_cursor: usize,
) -> (Decoder<RIBLTSymbol>, IbltPeelResult)
where
{
    tokio::task::spawn_blocking(move || {
        let mut decoder = decoder;
        decoder.try_decode();
        let result = peel_result(&decoder, remote_cursor);
        (decoder, result)
    })
    .await
    .unwrap()
}

/// Add a batch of coded symbols and peel, all on a blocking thread so the
/// CPU-heavy window application and decoding never block the async runtime.
pub async fn process_batch_blocking(
    decoder: Decoder<RIBLTSymbol>,
    symbols: Vec<RIBLTCodedSymbol>,
    remote_cursor: usize,
) -> (Decoder<RIBLTSymbol>, IbltPeelResult) {
    tokio::task::spawn_blocking(move || {
        let mut decoder = decoder;
        add_coded_symbols(&mut decoder, &symbols);
        decoder.try_decode();
        let result = peel_result(&decoder, remote_cursor);
        (decoder, result)
    })
    .await
    .unwrap()
}

/// Build a decoder seeded with the local symbol set on a blocking thread.
pub async fn build_decoder_blocking(symbols: HashSet<RIBLTSymbol>) -> Decoder<RIBLTSymbol> {
    tokio::task::spawn_blocking(move || {
        let mut decoder = Decoder::new();
        for symbol in symbols {
            decoder.add_symbol(symbol);
        }
        decoder
    })
    .await
    .unwrap()
}
