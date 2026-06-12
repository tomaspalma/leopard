# replication-engine

A modular Rust library and benchmarking harness for **set reconciliation between replicas** of a
key-value store. It provides several interchangeable reconciliation protocols behind a common
trait, a fluent builder DSL for launching multi-node topologies, and a metrics/experiment
pipeline for measuring and comparing them across a range of replica similarities.

The library is the engineering artifact behind a master's dissertation on efficient data
replication for large-scale systems. Its two goals are (1) to offer a protocol-agnostic framework
into which new reconciliation algorithms can be dropped, and (2) to benchmark those algorithms on
bytes transferred, round duration, CPU, and peak memory.

## Reconciliation protocols

Each protocol carries a stable wire identifier (the first 8 bytes of every message) defined in
`protocol::ProtocolId`:

| Protocol | `ProtocolChoice` | Wire ID | Summary |
|----------|------------------|:-------:|---------|
| Merkle tree | `Merkle` | 2 | Hash-tree probing baseline. |
| Rateless set reconciliation (RIBLT) | `Riblt` | 1 | Rateless invertible Bloom lookup tables; transfers ~1.35d–1.72d coded symbols for `d` differences. |
| Hybrid RBF + RIBLT | `RbfRiblt` | 3 | A rateless Bloom filter classifies elements, then RIBLT reconciles the shared candidate set. |
| Ribbon-filter + RIBLT | `RfRiblt` | 4 | Ribbon filter variant of the hybrid pre-pass. |

(ID `5` is reserved for the `Replication` protocol; `0` means "no protocol".)

## Architecture

The repository is a Cargo workspace. The root crate (`replication-engine`) is the CLI/experiment
driver; the reconciliation logic and supporting infrastructure live in member crates:

| Crate | Responsibility |
|-------|----------------|
| `reconciliation` | The reconciliation protocols (`merkle_tree`, `riblt`, `rbf_riblt`, `rf_riblt`), shared `riblt_core`, and the correctness `checker`. |
| `protocol` | The base `Protocol` trait and the `ProtocolId` enum (compile-time-unique wire IDs). |
| `dsl` | The fluent builder API: `NodeBuilder`, `ProtocolChoice`, `ServiceBuilder`, `CheckerBuilder`. |
| `connection` | Sockets, routing, and per-message handler registration. |
| `message` | Wire message framing (16-byte header: protocol ID + sender port) and the `impl_protocol_message!` macro. |
| `membership` | Neighbor/membership tracking. |
| `state` | Node and key-value `DataState`, including the change-listener interface protocols hook into. |
| `replication` | Replication protocol. |
| `runtime` | The global async `RUNTIME`, timing, and the metrics/CSV experiment recorder. |
| `node`, `config`, `services`, `errors`, `taints` | Node assembly, configuration, HTTP services, error types, and supporting utilities. |

A reconciliation protocol is implemented by satisfying two layered traits: the base `Protocol`
trait (a unique ID, a deserializer that routes raw bytes, and an `init` that registers the periodic
sync task plus per-message handlers) and the `ReconciliationProtocol` extension trait (an accessor
for the protocol's internal session state). See [`usage.md`](./usage.md) for the full walkthrough.

## Getting started

### Prerequisites

The project targets a Rust **nightly** toolchain. The easiest path is the provided Nix flake, which
pins the toolchain and the Python tooling used by the analysis scripts (`pandas`, `matplotlib`):

```bash
nix develop
```

Otherwise, install a recent Rust nightly and Python 3 with `pandas`/`matplotlib`.

### Build

```bash
cargo build --release
```

## Running an experiment

A run launches a set of nodes, each given an address, a TCP port, an HTTP port, and a JSON dataset,
then reconciles them under the chosen protocol. The CLI exposes this through the `custom-nodes`
subcommand:

```bash
cargo run --release -- \
  --run-id demo --trial 1 --similarity 0.5 --exit-on-reconciliation \
  custom-nodes --protocol riblt \
  --nodes "127.0.0.1,9000,3000,data/demo_node1.json" \
  --nodes "127.0.0.1,9001,3001,data/demo_node2.json"
```

- `--protocol` is one of `merkle`, `riblt`, `rbf_riblt`, `rf_riblt`.
- Each `--nodes` value is `ip,port,http_port,dataset`.
- `--exit-on-reconciliation` makes the process wait until every pair has converged, run the
  `ReconciliationChecker`, emit correctness metrics, and exit. Without it, nodes run indefinitely.

The `scripts/run_experiment.sh` helper wraps this for two-node runs over a dataset prefix:

```bash
scripts/run_experiment.sh riblt demo        # uses data/demo_node1.json, data/demo_node2.json
```

Datasets are plain `{"key": "value"}` JSON maps. Generate deterministic replica pairs at a target
similarity with:

```bash
python3 scripts/generate_data.py --size 100000 --similarity 0.5 --prefix demo
```

### Using the builder directly

```rust
use dsl::{CheckerBuilder, NodeBuilder, ProtocolChoice, ServiceBuilder};
use runtime::RUNTIME;

let result = NodeBuilder::new()
    .node().addr("127.0.0.1").port(9000)
        .dataset("data/demo_node1.json")
        .protocol(ProtocolChoice::Riblt)
        .service(ServiceBuilder::http().port(3000))
    .node().addr("127.0.0.1").port(9001)
        .dataset("data/demo_node2.json")
        .protocol(ProtocolChoice::Riblt)
        .service(ServiceBuilder::http().port(3001))
    .checker(CheckerBuilder::new().local())
    .build().await
    .expect("failed to build nodes");

for task in result.tasks {
    RUNTIME.write().unwrap().add_task(task).unwrap();
}
RUNTIME.write().unwrap().init().unwrap();
```

## Benchmarking

`scripts/run_similarity_sweep.sh` runs every protocol across a sweep of similarities and trials,
regenerating datasets per trial and collecting metrics:

```bash
PROTOCOLS="riblt,merkle,rbf_riblt" TRIALS=5 scripts/run_similarity_sweep.sh
```

Metrics are written as CSV under `metrics_output/<run_id>/`. The `scripts/analyze_*.py` and
`scripts/plot_*.py` tools turn those into comparison tables and figures (generated LaTeX tables land
under `metrics_output/analysis`).

> The sweep disables disk persistence (`DISABLE_STORAGE_FLUSH=1`) and lowers logging
> (`RUST_LOG=warn`) so I/O and logging do not contaminate the measured round duration and resource
> metrics.

## Repository layout

```
src/                CLI driver and standalone experiment binaries
<crate>/            workspace member crates (see Architecture)
riblt/              vendored rateless-IBLT crate
scripts/            data generation, experiment runners, analysis and plotting
data/               generated datasets (git-ignored)
metrics_output/     experiment results (git-ignored)
flake.nix           Nix dev shell (Rust nightly + Python tooling)
```

## License

See individual crate directories for license information.
