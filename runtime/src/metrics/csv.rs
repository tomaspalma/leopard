use metrics::{Key, KeyName, Metadata, Recorder, SharedString, Unit};
use metrics_util::registry::{AtomicStorage, Registry};
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::{OpenOptions, create_dir_all};
use tokio::io::AsyncWriteExt;
use tokio::time::interval;

#[derive(Clone)]
pub struct CsvRecorder {
    registry: Arc<Registry<Key, AtomicStorage>>,
}

impl CsvRecorder {
    pub fn new() -> Self {
        Self {
            registry: Arc::new(Registry::atomic()),
        }
    }

    pub fn start_exporter(self, directory: String, flush_interval: Duration) {
        let registry = self.registry.clone();

        crate::spawn(async move {
            let _ = create_dir_all(&directory).await;
            let mut interval = interval(flush_interval);

            loop {
                interval.tick().await;

                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64();

                let counters = registry.get_counter_handles();
                for (key, counter) in counters {
                    let val = counter.load(std::sync::atomic::Ordering::Relaxed);
                    let file_name = format!("{}/{}.csv", directory, key.name());

                    let labels: Vec<String> = key
                        .labels()
                        .map(|l| format!("{}={}", l.key(), l.value()))
                        .collect();
                    let labels_str = labels.join(";");

                    if let Ok(mut file) = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&file_name)
                        .await
                    {
                        if let Ok(metadata) = file.metadata().await {
                            if metadata.len() == 0 {
                                let _ = file.write_all(b"timestamp,value,labels\n").await;
                            }
                        }
                        let line = format!("{:.3},{},\"{}\"\n", timestamp, val, labels_str);
                        let _ = file.write_all(line.as_bytes()).await;
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
