pub trait PeriodTimeUnit {
    fn tick(&self) -> ();
}

pub struct TokioPeriodTimeUnit {}

impl PeriodTimeUnit for TokioPeriodTimeUnit {
    fn tick(&self) {}
}
