use std::collections::HashSet;

use riblt::{CodedSymbol, RatelessIBLT, UnmanagedRatelessIBLT};
use riblt::symbol::PeelableResult;
use state::node::{DefaultNodeState, NodeState};
use state::storage::item::DefaultDataStateItem;

use crate::riblt::messages::{RIBLTCodedSymbol, RIBLTSymbol};

pub struct IbltPeelResult {
    pub successful: bool,
    pub remote_symbols: Vec<RIBLTSymbol>,
    pub local_symbols: Vec<RIBLTSymbol>,
}

/// Build an IBLT symbol set from all items in the node's default storage.
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

/// Feed a batch of wire-format coded symbols into the receiving-side IBLT state and
/// return clones of both coded-symbol vectors, ready for collapse.
pub fn absorb_coded_symbols(
    remote_iblt: &mut UnmanagedRatelessIBLT<RIBLTSymbol>,
    local_iblt: &mut RatelessIBLT<RIBLTSymbol, HashSet<RIBLTSymbol>>,
    symbols: &[RIBLTCodedSymbol],
) -> (Vec<CodedSymbol<RIBLTSymbol>>, Vec<CodedSymbol<RIBLTSymbol>>) {
    for symbol in symbols {
        let mut cs = CodedSymbol::new();
        cs.sum = symbol.sum.clone();
        cs.hash = symbol.hash;
        cs.count = symbol.count;
        remote_iblt.add_coded_symbol(&cs);
    }
    let len = remote_iblt.coded_symbols.len();
    local_iblt.extend_coded_symbols(len);
    (
        local_iblt.coded_symbols.clone(),
        remote_iblt.coded_symbols.clone(),
    )
}

/// Collapse two IBLT coded-symbol vectors on a blocking thread and peel all
/// recoverable symbols, partitioned into remote-only and local-only sets.
pub async fn collapse_and_peel(
    local_coded_symbols: Vec<CodedSymbol<RIBLTSymbol>>,
    remote_coded_symbols: Vec<CodedSymbol<RIBLTSymbol>>,
) -> IbltPeelResult {
    tokio::task::spawn_blocking(move || {
        let local_iblt = UnmanagedRatelessIBLT {
            coded_symbols: local_coded_symbols,
        };
        let remote_iblt = UnmanagedRatelessIBLT {
            coded_symbols: remote_coded_symbols,
        };
        let mut collapsed = local_iblt.collapse(&remote_iblt);
        let peeled = collapsed.peel_all_symbols();
        let successful = collapsed.is_empty();

        let mut remote_symbols = Vec::new();
        let mut local_symbols = Vec::new();
        for symbol in peeled {
            match symbol {
                PeelableResult::Remote(s) => remote_symbols.push(s),
                PeelableResult::Local(s) => local_symbols.push(s),
                _ => {}
            }
        }

        IbltPeelResult {
            successful,
            remote_symbols,
            local_symbols,
        }
    })
    .await
    .unwrap()
}
