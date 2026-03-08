pub trait MessageDeserializer<A, R> {
    fn deserialize(&self, content: A) -> R;
}

pub struct RkyvDeserializer {}
