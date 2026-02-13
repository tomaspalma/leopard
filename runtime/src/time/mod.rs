use async_trait::async_trait;

#[async_trait]
pub trait PeriodTimeUnit {
    async fn tick(&self) -> ();
}

pub struct TokioPeriodTimeUnit {}

#[async_trait]
impl PeriodTimeUnit for TokioPeriodTimeUnit {
    async fn tick(&self) {}
}
