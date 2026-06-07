//! Empirical scaling of RIBLT decoding as the symmetric difference d grows.
//!
//! For each d, builds two disjoint sets (|A| = |B| = d/2, no overlap, so the
//! symmetric difference is exactly d), runs the *real* `Decoder` against the
//! *real* `Encoder`, and reports:
//!
//!   cells_needed   coded symbols consumed before decode succeeded (≈ 1.35·d)
//!   overhead       cells_needed / d
//!   peel_xor       total XOR-outs the peeler performs = sum over the d decoded
//!                  symbols of how many received cells each one maps to. This is
//!                  the O(d log d) term: each symbol touches O(log d) cells.
//!   peel_per_d     peel_xor / d  (grows ~ log d -> the superlinear factor)
//!   encode_ms      wall time generating the coded-symbol stream
//!   decode_ms      wall time of collapse + peel (add_coded_symbol + try_decode)
//!
//! Run:  cargo run --release --example decode_scaling
//!       cargo run --release --example decode_scaling -- 10 100 1000 10000 100000
//! Output is CSV on stdout (a human summary goes to stderr), so it pipes
//! straight into a plot.

use std::time::Instant;

use riblt::symbol::Symbol;
use riblt::{Decoder, Encoder, RandomMapping};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Num(u64);

impl Symbol for Num {
    const BYTE_ARRAY_LENGTH: usize = 8;
    fn encode_to_bytes(&self) -> Vec<u8> {
        self.0.to_le_bytes().to_vec()
    }
    fn decode_from_bytes(buffer: &Vec<u8>) -> Self {
        Num(u64::from_le_bytes(buffer[0..8].try_into().unwrap()))
    }
}

/// How many coded-symbol indices below `cells` this symbol's mapping lands in.
/// Uses the crate's RandomMapping, so it matches what the peeler actually touches.
fn cells_touched(hash: u64, cells: usize) -> usize {
    RandomMapping::from_hash(hash)
        .take_while(|&i| i < cells)
        .count()
}

fn run_one(d: u64) -> (usize, u64, u128, u128) {
    let half = d / 2;
    // Disjoint sets: A = [0, half), B = [half, 2*half). Difference = d.
    let a: Vec<Num> = (0..half).map(Num).collect();
    let b: Vec<Num> = (half..2 * half).map(Num).collect();

    let mut encoder = Encoder::new(b.iter().cloned());
    let mut decoder = Decoder::new();
    for s in &a {
        decoder.add_symbol(s.clone());
    }

    let cap = (d as usize) * 3 + 1000;
    let mut encode_ns: u128 = 0;
    let mut decode_ns: u128 = 0;
    let mut cells_needed = 0usize;

    for i in 0..cap {
        let t0 = Instant::now();
        let cs = encoder.get_coded_symbol(i);
        let t1 = Instant::now();
        decoder.add_coded_symbol(cs);
        decoder.try_decode();
        let t2 = Instant::now();
        encode_ns += (t1 - t0).as_nanos();
        decode_ns += (t2 - t1).as_nanos();
        if decoder.decoded() {
            cells_needed = i + 1;
            break;
        }
    }
    assert!(cells_needed > 0, "failed to decode d={d} within {cap} cells");

    // peel_xor: total cells touched across every decoded difference symbol.
    let peel_xor: u64 = a
        .iter()
        .chain(b.iter())
        .map(|s| cells_touched(s.hash_(), cells_needed) as u64)
        .sum();

    (cells_needed, peel_xor, encode_ns, decode_ns)
}

fn main() {
    let args: Vec<u64> = std::env::args()
        .skip(1)
        .filter_map(|a| a.parse().ok())
        .collect();
    let ds = if args.is_empty() {
        vec![10, 100, 1000, 10000, 100000]
    } else {
        args
    };

    println!("d,cells_needed,overhead,peel_xor,peel_per_d,encode_ms,decode_ms");
    eprintln!(
        "{:>8} {:>12} {:>8} {:>12} {:>10} {:>10} {:>10}",
        "d", "cells", "overhead", "peel_xor", "peel/d", "encode_ms", "decode_ms"
    );
    for d in ds {
        let (cells, peel_xor, enc_ns, dec_ns) = run_one(d);
        let overhead = cells as f64 / d as f64;
        let peel_per_d = peel_xor as f64 / d as f64;
        let enc_ms = enc_ns as f64 / 1e6;
        let dec_ms = dec_ns as f64 / 1e6;
        println!(
            "{d},{cells},{overhead:.3},{peel_xor},{peel_per_d:.3},{enc_ms:.3},{dec_ms:.3}"
        );
        eprintln!(
            "{:>8} {:>12} {:>8.3} {:>12} {:>10.3} {:>10.3} {:>10.3}",
            d, cells, overhead, peel_xor, peel_per_d, enc_ms, dec_ms
        );
    }
}
