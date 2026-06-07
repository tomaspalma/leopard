//! Transport-agnostic rateless-IBLT streaming engine.
//!
//! Both the standalone RIBLT protocol and the rbf_riblt scom phase drive this
//! engine. The engine owns the windowed sender loop, the credit/flow-control
//! state, and the in-order reassembly + decode loop. Everything host-specific —
//! how to put a message on the wire and what a decoded result means — is
//! supplied through the `RibltStreamTransport` and `RibltDecodeSink` traits.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use connection::node::port::NodeAddress;
use riblt::RatelessIBLT;
use tokio::sync::{mpsc, Notify, RwLock};
use tokio::time::{timeout, Duration};
use tracing::info;

use runtime::spawn;

use crate::riblt::messages::{RIBLTCodedSymbol, RIBLTSymbol};
use crate::riblt::session::{build_decoder_blocking, process_batch_blocking};

// Coded symbols carried in a single batch message.
const CHUNK_SIZE: usize = 30;
// Flow-control window: the maximum coded symbols kept in flight (sent but
// unacknowledged) bounds how far past the ~1.35*d decode point the sender can
// overshoot. A fixed, large window lets the sender outrun a slow decoder (which
// must first seed all n local symbols before it can peel anything) and flood a
// near-constant volume regardless of the real difference — at high similarity
// that is many times the whole dataset. Instead the window starts small and
// grows with how many symbols have already been sent: at high similarity the
// decoder finishes inside MIN_SEND_WINDOW, so the existing stop-on-decode bounds
// the overshoot to a handful of symbols; at low similarity the window expands
// toward MAX_SEND_WINDOW to keep the pipeline full. Overshoot is therefore a
// small fraction of the symbols actually needed rather than a fixed 4096.
const MIN_SEND_WINDOW: usize = 64;
const MAX_SEND_WINDOW: usize = 4096;
// The in-flight allowance grows as sent / SEND_WINDOW_GROWTH_DIVISOR, capping the
// wasted overshoot at roughly 1/divisor of the symbols genuinely transmitted.
const SEND_WINDOW_GROWTH_DIVISOR: usize = 4;
// Safety net so a lost acknowledgement can't wedge the sender forever.
const ACK_TIMEOUT: Duration = Duration::from_millis(5000);

/// How the host puts engine messages on the wire. Each protocol builds its own
/// message types (and uses its own protocol id) behind these calls.
#[async_trait]
pub trait RibltStreamTransport: Send + Sync + 'static {
    async fn send_symbols(
        &self,
        neighbor: &NodeAddress,
        session_id: &str,
        start_index: u64,
        symbols: Vec<RIBLTCodedSymbol>,
    );
    async fn send_request_more(&self, neighbor: &NodeAddress, session_id: &str, received_count: u64);
    async fn send_finished(&self, neighbor: &NodeAddress, session_id: &str);
}

/// What the host does with decoded data and how it seeds the local window.
#[async_trait]
pub trait RibltDecodeSink: Send + Sync + 'static {
    /// Local symbol set to seed a receiving decoder with (subtracted from the
    /// incoming stream). riblt uses the full store; rbf uses its s_com subset.
    async fn seed_symbols(&self, neighbor: &NodeAddress) -> HashSet<RIBLTSymbol>;
    /// Newly decoded remote-only symbols, delivered incrementally as they peel.
    async fn on_remote_symbols(
        &self,
        neighbor: &NodeAddress,
        session_id: &str,
        new_remote: Vec<RIBLTSymbol>,
    );
    /// Fired once when the session fully decodes, with the local-only set and the
    /// round duration. The host records metrics and runs any follow-up here.
    ///
    /// `seed_secs` is the time spent seeding the decoder with the local set
    /// (O(local set size)); `decode_secs` is the CPU time spent peeling coded
    /// symbols (O(difference)); `decoded_difference` is the total symmetric
    /// difference recovered (local-only + remote-only). These let the host
    /// attribute reconciliation cost between seeding and decoding.
    async fn on_complete(
        &self,
        neighbor: &NodeAddress,
        session_id: &str,
        local_only: Vec<RIBLTSymbol>,
        round_secs: f64,
        seed_secs: f64,
        decode_secs: f64,
        decoded_difference: usize,
    );
}

pub struct SendingState {
    pub local_iblt: RatelessIBLT<RIBLTSymbol>,
    pub start_time: Instant,
    pub session_id: String,
    pub resend_notify: Arc<Notify>,
    // Highest coded-symbol count the receiver has confirmed consuming. Monotonic.
    pub acked: usize,
}

pub struct ReceivingState {
    pub session_id: String,
    pub start_time: Instant,
    pub symbol_tx: mpsc::UnboundedSender<(u64, Vec<RIBLTCodedSymbol>)>,
}

pub struct RibltStreamEngine {
    transport: Arc<dyn RibltStreamTransport>,
    sink: Arc<dyn RibltDecodeSink>,
    pub sending_states: Arc<RwLock<HashMap<NodeAddress, SendingState>>>,
    pub receiving_states: Arc<RwLock<HashMap<NodeAddress, ReceivingState>>>,
}

impl RibltStreamEngine {
    pub fn new(
        transport: Arc<dyn RibltStreamTransport>,
        sink: Arc<dyn RibltDecodeSink>,
    ) -> Self {
        Self {
            transport,
            sink,
            sending_states: Arc::new(RwLock::new(HashMap::new())),
            receiving_states: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn already_sending(&self, neighbor: &NodeAddress) -> bool {
        self.sending_states.read().await.contains_key(neighbor)
    }

    /// Begin streaming `symbols` to `neighbor` under the credit window.
    pub async fn start_send(
        self: &Arc<Self>,
        neighbor: NodeAddress,
        symbols: HashSet<RIBLTSymbol>,
        session_id: String,
    ) {
        self.sending_states.write().await.insert(
            neighbor.clone(),
            SendingState {
                local_iblt: RatelessIBLT::new(symbols),
                start_time: Instant::now(),
                session_id,
                resend_notify: Arc::new(Notify::new()),
                acked: 0,
            },
        );
        let this = self.clone();
        spawn!({
            this.run_sender(neighbor).await;
        });
    }

    async fn run_sender(self: Arc<Self>, neighbor: NodeAddress) {
        info!("Streaming riblt symbols to {:?}", neighbor);
        let mut sent: usize = 0;

        loop {
            let (acked, resend_notify, session_id) = {
                let guard = self.sending_states.read().await;
                match guard.get(&neighbor) {
                    Some(status) => (
                        status.acked,
                        status.resend_notify.clone(),
                        status.session_id.clone(),
                    ),
                    // Session removed (peer finished decoding) -> stop.
                    None => break,
                }
            };

            // Grow the allowed in-flight window with the symbols already sent so
            // the sender stays close to the decoder when the difference is small
            // (so stop-on-decode bounds the overshoot) but still fills the pipe
            // when the difference is large.
            let window = (sent / SEND_WINDOW_GROWTH_DIVISOR)
                .clamp(MIN_SEND_WINDOW, MAX_SEND_WINDOW);
            let in_flight = sent.saturating_sub(acked);
            if in_flight >= window {
                let _ = timeout(ACK_TIMEOUT, resend_notify.notified()).await;
                continue;
            }

            let budget = (window - in_flight).min(CHUNK_SIZE);
            let start_index = sent;
            let mut symbols = Vec::with_capacity(budget);
            {
                let mut guard = self.sending_states.write().await;
                let status = match guard.get_mut(&neighbor) {
                    Some(s) => s,
                    None => break,
                };
                for _ in 0..budget {
                    let cs = status.local_iblt.next_coded_symbol();
                    symbols.push(RIBLTCodedSymbol {
                        sum: cs.sum,
                        hash: cs.hash,
                        count: cs.count,
                    });
                }
            }
            sent += budget;

            self.transport
                .send_symbols(&neighbor, &session_id, start_index as u64, symbols)
                .await;
        }

        info!("Stopped streaming to {:?} after {} symbols", neighbor, sent);
    }

    /// An acknowledgement advanced the receiver's consumed count: slide the
    /// window and wake the sender.
    pub async fn on_request_more(
        &self,
        neighbor: &NodeAddress,
        session_id: &str,
        received_count: u64,
    ) {
        if let Some(status) = self.sending_states.write().await.get_mut(neighbor) {
            if status.session_id == session_id {
                let ack = received_count as usize;
                if ack > status.acked {
                    status.acked = ack;
                }
                status.resend_notify.notify_one();
            }
        }
    }

    /// The peer finished decoding our stream: tear down the sending session and
    /// wake the sender so it observes the removal and stops. Returns true if a
    /// matching session was actually stopped.
    pub async fn on_finished(&self, neighbor: &NodeAddress, session_id: &str) -> bool {
        let mut guard = self.sending_states.write().await;
        match guard.get(neighbor) {
            Some(status) if status.session_id == session_id => {
                let notify = status.resend_notify.clone();
                guard.remove(neighbor);
                notify.notify_one();
                true
            }
            _ => false,
        }
    }

    /// Feed a received batch to the per-session decode task, creating that task
    /// (and seeding its decoder) on first sight of a session.
    pub async fn on_symbols(
        self: &Arc<Self>,
        neighbor: NodeAddress,
        session_id: String,
        start_index: u64,
        symbols: Vec<RIBLTCodedSymbol>,
    ) {
        let tx = {
            let mut guard = self.receiving_states.write().await;
            match guard.get(&neighbor) {
                Some(status) if status.session_id == session_id => status.symbol_tx.clone(),
                _ => {
                    // New session (or a session change): replacing the entry drops
                    // any previous sender, ending the stale decode task.
                    let (tx, rx) = mpsc::unbounded_channel();
                    let start_time = Instant::now();
                    guard.insert(
                        neighbor.clone(),
                        ReceivingState {
                            session_id: session_id.clone(),
                            start_time,
                            symbol_tx: tx.clone(),
                        },
                    );
                    let this = self.clone();
                    let nb = neighbor.clone();
                    let sid = session_id.clone();
                    spawn!({
                        this.run_receiver(nb, sid, start_time, rx).await;
                    });
                    tx
                }
            }
        };
        let _ = tx.send((start_index, symbols));
    }

    async fn run_receiver(
        self: Arc<Self>,
        neighbor: NodeAddress,
        session_id: String,
        start_time: Instant,
        mut rx: mpsc::UnboundedReceiver<(u64, Vec<RIBLTCodedSymbol>)>,
    ) {
        let seed_start = Instant::now();
        let local = self.sink.seed_symbols(&neighbor).await;
        let mut decoder = build_decoder_blocking(local).await;
        let seed_secs = seed_start.elapsed().as_secs_f64();
        let mut decode_secs = 0f64;
        let mut stored_remote: usize = 0;

        // The decoder is positional: the k-th symbol fed must be encoder index k.
        // Batches can arrive out of order, so buffer by start index and feed only
        // the contiguous run from `next_index`.
        let mut reorder: BTreeMap<u64, Vec<RIBLTCodedSymbol>> = BTreeMap::new();
        let mut next_index: u64 = 0;

        while let Some((start, symbols)) = rx.recv().await {
            reorder.insert(start, symbols);
            while let Ok((s, v)) = rx.try_recv() {
                reorder.insert(s, v);
            }

            let mut batch = Vec::new();
            while let Some(chunk) = reorder.remove(&next_index) {
                next_index += chunk.len() as u64;
                batch.extend(chunk);
            }
            if batch.is_empty() {
                continue;
            }
            let received_count = next_index;

            let decode_start = Instant::now();
            let (next_decoder, peel) =
                process_batch_blocking(decoder, batch, stored_remote).await;
            decode_secs += decode_start.elapsed().as_secs_f64();
            decoder = next_decoder;
            stored_remote = peel.remote_total;

            if !peel.remote_symbols.is_empty() {
                self.sink
                    .on_remote_symbols(&neighbor, &session_id, peel.remote_symbols)
                    .await;
            }

            if peel.successful {
                let decoded_difference = peel.local_symbols.len() + peel.remote_total;
                self.transport.send_finished(&neighbor, &session_id).await;
                self.sink
                    .on_complete(
                        &neighbor,
                        &session_id,
                        peel.local_symbols,
                        start_time.elapsed().as_secs_f64(),
                        seed_secs,
                        decode_secs,
                        decoded_difference,
                    )
                    .await;
                break;
            }

            self.transport
                .send_request_more(&neighbor, &session_id, received_count)
                .await;
        }
    }
}
