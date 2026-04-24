use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, Interval, interval_at};

#[async_trait]
pub trait PeriodTimeUnit {
    async fn tick(&self) -> ();
}

pub struct TokioPeriodTimeUnit {
    interval: Mutex<Interval>,
}

impl TokioPeriodTimeUnit {
    pub fn new(duration: Duration) -> Self {
        Self {
            interval: Mutex::new(interval_at(Instant::now() + Duration::from_secs(1), duration)),
        }
    }
}

#[async_trait]
impl PeriodTimeUnit for TokioPeriodTimeUnit {
    async fn tick(&self) {
        let mut interval = self.interval.lock().await;

        interval.tick().await;
    }
}
