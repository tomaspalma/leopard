pub mod rbf;
pub mod ribbon;

pub trait SimilarityLevelDegree<D> {
    fn degree(&self) -> D;
}

pub trait SimilarityLevelDetector<D> {
    fn degree(&self) -> D;
}

pub enum DefaultSimilarityLevel {
    Low,
    Medium,
    High,
}

pub struct DefaultSimilartyLevelDetector {}

impl DefaultSimilartyLevelDetector {
    pub fn new() -> Self {
        Self {}
    }
}

impl SimilarityLevelDetector<DefaultSimilarityLevel> for DefaultSimilartyLevelDetector {
    fn degree(&self) -> DefaultSimilarityLevel {
        DefaultSimilarityLevel::Low
    }
}
