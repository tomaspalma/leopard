pub mod node;
pub mod runner;

pub trait Builder<T> {
    fn build(self) -> T;
}
