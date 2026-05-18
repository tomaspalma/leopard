use std::collections::BinaryHeap;

use crate::mapping::RandomMapping;
use crate::symbol::{CodedSymbol, HashedSymbol, Symbol};

// ─── Heap entry ───────────────────────────────────────────────────────────────

#[derive(Eq, PartialEq)]
struct SymbolMapping {
    coded_idx: usize,
    source_idx: usize,
}

// Reverse the natural ordering so BinaryHeap becomes a min-heap keyed by coded_idx.
impl Ord for SymbolMapping {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.coded_idx.cmp(&self.coded_idx)
            .then(other.source_idx.cmp(&self.source_idx))
    }
}

impl PartialOrd for SymbolMapping {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// ─── CodingWindow ─────────────────────────────────────────────────────────────

// Shared core for both Encoder and Decoder.
// Tracks a set of symbols and efficiently maps them onto coded symbol indices
// using a min-heap ordered by each symbol's next pending coded-symbol index.
struct CodingWindow<T: Symbol> {
    symbols: Vec<HashedSymbol<T>>,
    mappings: Vec<RandomMapping>,
    queue: BinaryHeap<SymbolMapping>,
    next_idx: usize,
}

impl<T: Symbol> CodingWindow<T> {
    fn new() -> Self {
        Self {
            symbols: Vec::new(),
            mappings: Vec::new(),
            queue: BinaryHeap::new(),
            next_idx: 0,
        }
    }

    fn add_symbol(&mut self, s: T) {
        let hash = s.hash_();
        self.add_hashed_symbol(HashedSymbol { symbol: s, hash });
    }

    fn add_hashed_symbol(&mut self, hs: HashedSymbol<T>) {
        // Consume the first mapping index (always 0) to advance the iterator
        // past it, leaving the mapping ready to yield the second index on the
        // next call.  The heap entry carries the initial coded_idx of 0.
        let mut mapping = RandomMapping::from_hash(hs.hash);
        let first_idx = mapping.next().unwrap();
        self.add_hashed_symbol_at(hs, first_idx, mapping);
    }

    // Insert a symbol whose mapping has already been fast-forwarded: first_idx
    // is the next coded-symbol index it needs to be applied to, and mapping is
    // positioned one step beyond that.  Used by Decoder when adding a freshly
    // peeled symbol to the remote/local sub-windows.
    fn add_hashed_symbol_at(
        &mut self,
        hs: HashedSymbol<T>,
        first_idx: usize,
        mapping: RandomMapping,
    ) {
        let source_idx = self.symbols.len();
        self.symbols.push(hs);
        self.mappings.push(mapping);
        self.queue.push(SymbolMapping { coded_idx: first_idx, source_idx });
    }

    // Apply every symbol that maps to `next_idx` to `cs`, then advance next_idx.
    // `direction` is +1 (add) or -1 (remove).
    fn apply_window(&mut self, mut cs: CodedSymbol<T>, direction: i64) -> CodedSymbol<T> {
        while let Some(top) = self.queue.peek() {
            if top.coded_idx != self.next_idx {
                break;
            }
            let sm = self.queue.pop().unwrap();
            cs.apply_hashed(&self.symbols[sm.source_idx], direction);
            let next_coded_idx = self.mappings[sm.source_idx].next().unwrap();
            self.queue.push(SymbolMapping { coded_idx: next_coded_idx, source_idx: sm.source_idx });
        }
        self.next_idx += 1;
        cs
    }

    fn reset(&mut self) {
        self.symbols.clear();
        self.mappings.clear();
        self.queue.clear();
        self.next_idx = 0;
    }
}

// ─── Encoder ──────────────────────────────────────────────────────────────────

/// Incrementally generates the infinite coded-symbol sequence for a fixed set.
/// Symbols must be added before the first call to `get_coded_symbol`.
pub struct Encoder<T: Symbol> {
    window: CodingWindow<T>,
    coded_symbols: Vec<CodedSymbol<T>>,
}

impl<T: Symbol> Encoder<T> {
    pub fn new(items: impl IntoIterator<Item = T>) -> Self {
        let mut enc = Self { window: CodingWindow::new(), coded_symbols: Vec::new() };
        for item in items {
            enc.window.add_symbol(item);
        }
        enc
    }

    pub fn add_symbol(&mut self, s: T) {
        assert!(
            self.coded_symbols.is_empty(),
            "add_symbol called after encoding started"
        );
        self.window.add_symbol(s);
    }

    /// Return the coded symbol at `index`, generating and caching all preceding
    /// symbols if necessary.
    pub fn get_coded_symbol(&mut self, index: usize) -> CodedSymbol<T> {
        while self.coded_symbols.len() <= index {
            let cs = self.window.apply_window(CodedSymbol::new(), 1);
            self.coded_symbols.push(cs);
        }
        self.coded_symbols[index].clone()
    }

    pub fn reset(&mut self) {
        self.window.reset();
        self.coded_symbols.clear();
    }
}

// ─── Decoder ──────────────────────────────────────────────────────────────────

/// Decodes the symmetric difference between a local set and a remote set.
///
/// Call `add_symbol` for every element of the local set, then feed remote coded
/// symbols one at a time with `add_coded_symbol`.  After each batch, call
/// `try_decode` to peel as many symbols as possible.  `decoded()` returns true
/// when every received coded symbol has been resolved.
pub struct Decoder<T: Symbol> {
    cs: Vec<CodedSymbol<T>>,     // collapsed coded symbols received so far
    window: CodingWindow<T>,     // local set (subtracted from incoming symbols)
    remote: CodingWindow<T>,     // symbols decoded as remote-only
    local: CodingWindow<T>,      // symbols decoded as local-only
    decodable: Vec<usize>,       // indices into cs that are ready to peel
    decoded: usize,
}

impl<T: Symbol> Decoder<T> {
    pub fn new() -> Self {
        Self {
            cs: Vec::new(),
            window: CodingWindow::new(),
            remote: CodingWindow::new(),
            local: CodingWindow::new(),
            decodable: Vec::new(),
            decoded: 0,
        }
    }

    pub fn add_symbol(&mut self, s: T) {
        assert!(self.cs.is_empty(), "add_symbol called after decoding started");
        self.window.add_symbol(s);
    }

    /// Feed the next remote coded symbol.  The symbol is collapsed with the
    /// local window on-the-fly; the result is stored internally.
    pub fn add_coded_symbol(&mut self, mut c: CodedSymbol<T>) {
        c = self.window.apply_window(c, -1);
        c = self.remote.apply_window(c, -1);
        c = self.local.apply_window(c, 1);
        if c.is_peelable() || (c.count == 0 && c.hash == 0) {
            self.decodable.push(self.cs.len());
        }
        self.cs.push(c);
    }

    /// Attempt to peel all currently decodable symbols, propagating each peel
    /// to all stored coded symbols and checking whether that makes more symbols
    /// decodable.
    pub fn try_decode(&mut self) {
        let mut i = 0;
        while i < self.decodable.len() {
            let cidx = self.decodable[i];
            let c = self.cs[cidx].clone();
            match c.count {
                1 => {
                    let symbol = T::decode_from_bytes(&c.sum);
                    let hs = HashedSymbol { hash: c.hash, symbol };
                    let (next_idx, mapping) = self.apply_new_symbol(&hs, -1);
                    self.remote.add_hashed_symbol_at(hs, next_idx, mapping);
                    self.decoded += 1;
                }
                -1 => {
                    let symbol = T::decode_from_bytes(&c.sum);
                    let hs = HashedSymbol { hash: c.hash, symbol };
                    let (next_idx, mapping) = self.apply_new_symbol(&hs, 1);
                    self.local.add_hashed_symbol_at(hs, next_idx, mapping);
                    self.decoded += 1;
                }
                0 => {
                    self.decoded += 1;
                }
                _ => panic!("invalid degree for decodable coded symbol: {}", c.count),
            }
            i += 1;
        }
        self.decodable.clear();
    }

    /// True when every coded symbol received so far has been decoded.
    pub fn decoded(&self) -> bool {
        self.decoded == self.cs.len()
    }

    /// Symbols present in the remote set but not the local set.
    pub fn remote_symbols(&self) -> &[HashedSymbol<T>] {
        &self.remote.symbols
    }

    /// Symbols present in the local set but not the remote set.
    pub fn local_symbols(&self) -> &[HashedSymbol<T>] {
        &self.local.symbols
    }

    pub fn reset(&mut self) {
        self.cs.clear();
        self.window.reset();
        self.remote.reset();
        self.local.reset();
        self.decodable.clear();
        self.decoded = 0;
    }

    // Apply a newly peeled symbol to every already-received coded symbol,
    // checking whether any become newly decodable.  Returns (first_pending_idx,
    // mapping) where first_pending_idx is the first coded-symbol index >= len(cs)
    // that this symbol maps to, and mapping is positioned one step past it.
    fn apply_new_symbol(&mut self, hs: &HashedSymbol<T>, direction: i64) -> (usize, RandomMapping) {
        let mut mapping = RandomMapping::from_hash(hs.hash);
        let mut idx = mapping.next().unwrap();
        while idx < self.cs.len() {
            self.cs[idx].apply_hashed(hs, direction);
            if self.cs[idx].is_peelable() {
                self.decodable.push(idx);
            }
            idx = mapping.next().unwrap();
        }
        (idx, mapping)
    }
}

// ─── Backward-compatibility aliases ───────────────────────────────────────────

pub type RatelessIBLT<T> = Encoder<T>;

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::SimpleSymbol;
    use std::collections::HashSet;

    #[test]
    fn test_collapsing() {
        let items_local: HashSet<SimpleSymbol> = HashSet::from([
            SimpleSymbol { value: 7 },
            SimpleSymbol { value: 15 },
            SimpleSymbol { value: 16 },
            SimpleSymbol { value: 2 },
        ]);

        let items_remote: HashSet<SimpleSymbol> = HashSet::from([
            SimpleSymbol { value: 7 },
            SimpleSymbol { value: 15 },
            SimpleSymbol { value: 16 },
            SimpleSymbol { value: 1 },
        ]);

        let local_only: HashSet<SimpleSymbol> =
            items_local.difference(&items_remote).cloned().collect();
        let remote_only: HashSet<SimpleSymbol> =
            items_remote.difference(&items_local).cloned().collect();

        let mut encoder = Encoder::new(items_remote.clone());
        let mut decoder = Decoder::new();
        for item in &items_local {
            decoder.add_symbol(item.clone());
        }

        let mut decoded = false;
        for i in 0..200 {
            let cs = encoder.get_coded_symbol(i);
            decoder.add_coded_symbol(cs);
            decoder.try_decode();
            if decoder.decoded() {
                decoded = true;
                break;
            }
        }

        assert!(decoded, "failed to decode within 200 coded symbols");

        let peeled_remote: HashSet<SimpleSymbol> =
            decoder.remote_symbols().iter().map(|hs| hs.symbol.clone()).collect();
        let peeled_local: HashSet<SimpleSymbol> =
            decoder.local_symbols().iter().map(|hs| hs.symbol.clone()).collect();

        assert_eq!(remote_only, peeled_remote);
        assert_eq!(local_only, peeled_local);
    }

    #[test]
    fn test_peeling() {
        let items: HashSet<SimpleSymbol> = HashSet::from([
            SimpleSymbol { value: 7 },
            SimpleSymbol { value: 15 },
            SimpleSymbol { value: 16 },
        ]);

        // Encoder has the items; decoder has an empty local set (all items are remote-only).
        let mut encoder = Encoder::new(items.clone());
        let mut decoder = Decoder::<SimpleSymbol>::new();

        let mut decoded = false;
        for i in 0..200 {
            let cs = encoder.get_coded_symbol(i);
            decoder.add_coded_symbol(cs);
            decoder.try_decode();
            if decoder.decoded() {
                decoded = true;
                break;
            }
        }

        assert!(decoded, "failed to decode within 200 coded symbols");

        let peeled_remote: HashSet<SimpleSymbol> =
            decoder.remote_symbols().iter().map(|hs| hs.symbol.clone()).collect();
        assert!(decoder.local_symbols().is_empty());
        assert_eq!(items, peeled_remote);
    }
}
