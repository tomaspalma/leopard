use crate::metrics::experiment::get_context;
use crate::metrics::resource::process_usage_snapshot;
use metrics::gauge;
use metrics::{Key, KeyName, Metadata, Recorder, SharedString, Unit};
use metrics_util::registry::{AtomicStorage, Registry};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::fs::{OpenOptions, create_dir_all};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Notify, broadcast};

static EXPECTED_PAIRS: AtomicUsize = AtomicUsize::new(0);

fn global_registry() -> &'static OnceLock<Arc<Registry<Key, AtomicStorage>>> {
    static REGISTRY: OnceLock<Arc<Registry<Key, AtomicStorage>>> = OnceLock::new();
    &REGISTRY
}

pub async fn flush_untagged_metrics(directory: &str) {
    let Some(registry) = global_registry().get() else {
        return;
    };
    let _ = create_dir_all(directory).await;

    let gauges = registry.get_gauge_handles();
    for (key, gauge) in gauges {
        if key
            .labels()
            .any(|l| l.key() == "neighbor" || l.key() == "target")
        {
            continue;
        }
        let val = f64::from_bits(gauge.load(Ordering::Relaxed)).to_string();
        CsvRecorder::write_metric_line(directory, &key, 1, 0, val).await;
    }

    let counters = registry.get_counter_handles();
    for (key, counter) in counters {
        if key
            .labels()
            .any(|l| l.key() == "neighbor" || l.key() == "target")
        {
            continue;
        }
        let val = counter.swap(0, Ordering::Relaxed).to_string();
        CsvRecorder::write_metric_line(directory, &key, 1, 0, val).await;
    }
}

fn completed_pairs() -> &'static Mutex<HashSet<(String, String)>> {
    static PAIRS: OnceLock<Mutex<HashSet<(String, String)>>> = OnceLock::new();
    PAIRS.get_or_init(|| Mutex::new(HashSet::new()))
}

fn shutdown_notify() -> &'static Notify {
    static NOTIFY: OnceLock<Notify> = OnceLock::new();
    NOTIFY.get_or_init(Notify::new)
}

pub fn shutdown_complete_notify() -> &'static Notify {
    static NOTIFY: OnceLock<Notify> = OnceLock::new();
    NOTIFY.get_or_init(Notify::new)
}

pub fn set_expected_pairs(n: usize) {
    EXPECTED_PAIRS.store(n, Ordering::Relaxed);
}

pub fn export_trigger() -> broadcast::Sender<String> {
    static SENDER: OnceLock<broadcast::Sender<String>> = OnceLock::new();
    SENDER.get_or_init(|| broadcast::channel(100).0).clone()
}

pub fn finish_iteration(from: String, target: String, protocol: &str) {
    if let Some(usage) = process_usage_snapshot() {
        let context = get_context();
        gauge!(
            "process_cpu_delta_seconds",
            "target" => target.clone(),
            "protocol" => protocol.to_string(),
            "run_id" => context.run_id().to_string(),
            "trial" => context.trial().to_string(),
            "similarity" => context.similarity().to_string()
        )
        .set(usage.cpu_delta_seconds);

        gauge!(
            "process_rss_memory_bytes",
            "target" => target.clone(),
            "protocol" => protocol.to_string(),
            "run_id" => context.run_id().to_string(),
            "trial" => context.trial().to_string(),
            "similarity" => context.similarity().to_string()
        )
        .set(usage.rss_bytes as f64);
    }

    let _ = export_trigger().send(target.clone());

    let expected = EXPECTED_PAIRS.load(Ordering::Relaxed);
    if expected > 0 {
        let mut pairs = completed_pairs().lock().unwrap();
        pairs.insert((from, target));
        if pairs.len() >= expected {
            shutdown_notify().notify_one();
        }
    }
}

#[derive(Clone)]
pub struct CsvRecorder {
    registry: Arc<Registry<Key, AtomicStorage>>,
}

impl CsvRecorder {
    pub fn new() -> Self {
        let registry = Arc::new(Registry::atomic());
        let _ = global_registry().set(Arc::clone(&registry));
        Self { registry }
    }

    fn format_label(value: &str) -> String {
        if value.starts_with("NodeAddress {") {
            let host_start = value.find("host: \"").unwrap_or(0) + 7;
            let host_end = value[host_start..].find("\"").unwrap_or(0) + host_start;
            let host = &value[host_start..host_end];

            let port_start = value.find("port: ").unwrap_or(0) + 6;
            let port_end = value[port_start..]
                .find(" }")
                .map(|i| i + port_start)
                .unwrap_or(value.len());
            let port = &value[port_start..port_end];

            format!("{}:{}", host, port)
        } else {
            value.to_string()
        }
    }

    async fn write_metric_line(
        directory: &str,
        key: &Key,
        iteration: usize,
        timestamp: u128,
        val: String,
    ) {
        let file_name = format!("{}/{}.csv", directory, key.name());

        let labels: Vec<(String, String)> = key
            .labels()
            .map(|l| {
                let formatted_value = Self::format_label(l.value());
                (l.key().to_string(), formatted_value)
            })
            .collect();

        let label_map: HashMap<String, String> = labels.iter().cloned().collect();
        let node_label = label_map
            .get("target")
            .or_else(|| label_map.get("node"))
            .or_else(|| label_map.get("neighbor"))
            .cloned()
            .unwrap_or_default();
        let protocol = label_map.get("protocol").cloned().unwrap_or_default();
        let run_id = label_map.get("run_id").cloned().unwrap_or_default();
        let trial = label_map.get("trial").cloned().unwrap_or_default();
        let similarity = label_map.get("similarity").cloned().unwrap_or_default();

        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_name)
            .await
        {
            if let Ok(metadata) = file.metadata().await {
                if metadata.len() == 0 {
                    let _ = file
                        .write_all(
                            b"iteration,timestamp,value,node,protocol,run_id,trial,similarity,labels\n",
                        )
                        .await;
                }
            }
            let line = format!(
                "{},{},{},\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\"\n",
                iteration,
                timestamp,
                val,
                node_label,
                protocol,
                run_id,
                trial,
                similarity,
                node_label
            );
            if let Err(e) = file.write_all(line.as_bytes()).await {
                println!("Error writing to file {}: {}", file_name, e);
            }
        } else {
            println!("Failed to open or create file: {}", file_name);
        }
    }

    async fn export_counters(
        registry: &Arc<Registry<Key, AtomicStorage>>,
        directory: &str,
        iteration: usize,
        timestamp: u128,
        target: &str,
    ) {
        let counters = registry.get_counter_handles();
        for (key, counter) in counters {
            if key
                .labels()
                .any(|l| l.value() == target || Self::format_label(l.value()) == target)
            {
                let val = counter
                    .swap(0, std::sync::atomic::Ordering::Relaxed)
                    .to_string();
                Self::write_metric_line(directory, &key, iteration, timestamp, val).await;
            }
        }
    }

    async fn export_gauges(
        registry: &Arc<Registry<Key, AtomicStorage>>,
        directory: &str,
        iteration: usize,
        timestamp: u128,
        target: &str,
    ) {
        let gauges = registry.get_gauge_handles();
        for (key, gauge) in gauges {
            if key
                .labels()
                .any(|l| l.value() == target || Self::format_label(l.value()) == target)
            {
                let raw = gauge.load(std::sync::atomic::Ordering::Relaxed);
                let val = f64::from_bits(raw).to_string();
                Self::write_metric_line(directory, &key, iteration, timestamp, val).await;
            }
        }
    }

    pub fn start_exporter(self, directory: String) {
        let registry = self.registry.clone();

        tokio::spawn(async move {
            let _ = create_dir_all(&directory).await;
            let start_time = std::time::Instant::now();
            let mut rx = export_trigger().subscribe();
            let mut iterations = std::collections::HashMap::new();

            loop {
                tokio::select! {
                    result = rx.recv() => {
                        if let Ok(target) = result {
                            let iteration = iterations.entry(target.clone()).or_insert(1);
                            let current_iteration = *iteration;
                            *iteration += 1;
                            let timestamp = start_time.elapsed().as_millis();
                            Self::export_counters(&registry, &directory, current_iteration, timestamp, &target).await;
                            Self::export_gauges(&registry, &directory, current_iteration, timestamp, &target).await;
                        }
                    }
                    _ = shutdown_notify().notified() => {
                        while let Ok(target) = rx.try_recv() {
                            let iteration = iterations.entry(target.clone()).or_insert(1);
                            let current_iteration = *iteration;
                            *iteration += 1;
                            let timestamp = start_time.elapsed().as_millis();
                            Self::export_counters(&registry, &directory, current_iteration, timestamp, &target).await;
                            Self::export_gauges(&registry, &directory, current_iteration, timestamp, &target).await;
                        }
                        shutdown_complete_notify().notify_one();
                        return;
                    }
                }
            }
        });
    }
}

impl Recorder for CsvRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> metrics::Counter {
        self.registry
            .get_or_create_counter(key, |c| c.clone().into())
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> metrics::Gauge {
        self.registry.get_or_create_gauge(key, |g| g.clone().into())
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> metrics::Histogram {
        self.registry
            .get_or_create_histogram(key, |h| h.clone().into())
    }
}
