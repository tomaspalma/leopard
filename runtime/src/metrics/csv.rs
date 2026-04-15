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

        tokio::spawn(async move {
            println!("Metrics exporter started for directory: {}", directory);
            let _ = create_dir_all(&directory).await;
            let mut interval = interval(flush_interval);
            let start_time = std::time::Instant::now();

            loop {
                interval.tick().await;

                let timestamp = start_time.elapsed().as_millis();

                let counters = registry.get_counter_handles();
                if counters.is_empty() {
                    // println!("No metrics to export yet.");
                }

                for (key, counter) in counters {
                    let val = counter.load(std::sync::atomic::Ordering::Relaxed);
                    let file_name = format!("{}/{}.csv", directory, key.name());

                    let labels: Vec<String> = key
                        .labels()
                        .map(|l| {
                            let value = l.value();
                            let formatted_value = if value.starts_with("NodeAddress {") {
                                let host_start = value.find("host: \"").unwrap_or(0) + 7;
                                let host_end =
                                    value[host_start..].find("\"").unwrap_or(0) + host_start;
                                let host = &value[host_start..host_end];

                                let port_start = value.find("port: ").unwrap_or(0) + 6;
                                let port_end =
                                    value[port_start..].find(" }").map(|i| i + port_start).unwrap_or(value.len());
                                let port = &value[port_start..port_end];

                                format!("{}:{}", host, port)
                            } else {
                                value.to_string()
                            };
                            format!("{}={}", l.key(), formatted_value)
                        })
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
                        let line = format!("{},{},\"{}\"\n", timestamp, val, labels_str);
                        if let Err(e) = file.write_all(line.as_bytes()).await {
                            println!("Error writing to file {}: {}", file_name, e);
                        }
                    } else {
                        println!("Failed to open or create file: {}", file_name);
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
