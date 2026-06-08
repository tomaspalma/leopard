//! Peak-RSS attribution profiler for the Merkle protocol's in-memory structures.
//!
//! Standalone experiment binary. Peak RSS (`VmHWM`) is a process-lifetime
//! high-water mark, so to attribute it to individual structures we build *one*
//! structure per process invocation and diff the peaks across runs. This uses the
//! same `VmHWM` metric the experiments report (see runtime/src/metrics/resource.rs),
//! and contains no unsafe code.
//!
//! Modes (argv[1]):
//!   baseline  - load the entries Vec only
//!   map       - entries + BTreeMap<String,String>  (mirrors BinaryMerkleTree.data)
//!   tree      - entries + BinaryMerkleTree         (internal map + boxed-node tree)
//!   snapshot  - entries + tree + a session snapshot (deep clone of the tree)
//!
//! Driver: scripts/merkle_mem_profile.sh runs every mode and prints the deltas.
//! Single run:
//!   cargo run --release --bin merkle_mem_profile -- tree data/memprof_node1.json

use std::collections::BTreeMap;
use std::hint::black_box;

use reconciliation::merkle_tree::tree::BinaryMerkleTree;

/// Peak resident set size (high-water mark) in bytes, read from /proc.
fn peak_rss_bytes() -> u64 {
    let content = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            if let Some(kb) = rest.split_whitespace().next() {
                if let Ok(kb) = kb.parse::<u64>() {
                    return kb * 1024;
                }
            }
        }
    }
    0
}

fn mb(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// Minimal parser for the generated `{ "k": "v", ... }` data files: one entry
/// per line, `"key": "value"` with a trailing comma. Avoids pulling in serde_json.
fn load_entries(path: &str) -> Vec<(String, String)> {
    let text = std::fs::read_to_string(path).expect("read data file");
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim().trim_end_matches(',');
        if !line.starts_with('"') {
            continue; // skip { and }
        }
        if let Some(sep) = line.find("\": \"") {
            let k = &line[1..sep];
            let v = &line[sep + 4..line.len() - 1];
            out.push((k.to_string(), v.to_string()));
        }
    }
    out
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "tree".to_string());
    let path = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "data/memprof_node1.json".to_string());

    let entries = load_entries(&path);
    let n = entries.len();

    match mode.as_str() {
        "baseline" => {
            black_box(&entries);
        }
        "map" => {
            let map: BTreeMap<String, String> = entries.iter().cloned().collect();
            black_box(&map);
        }
        "tree" => {
            let tree = BinaryMerkleTree::new();
            tree.replace_all(entries.clone());
            let _ = tree.get_root_hash();
            black_box(&tree);
        }
        "snapshot" => {
            let tree = BinaryMerkleTree::new();
            tree.replace_all(entries.clone());
            let snap = tree.snapshot();
            let _ = snap.get_root_hash();
            black_box(&tree);
            black_box(&snap);
        }
        other => {
            eprintln!("unknown mode {other:?}; use baseline|map|tree|snapshot");
            std::process::exit(2);
        }
    }

    black_box(&entries);
    // machine-readable line: <mode> <n> <peak_rss_bytes> <peak_mb>
    let peak = peak_rss_bytes();
    println!("{mode} {n} {peak} {:.1}", mb(peak));
}
