pub trait DataStateItem {}

pub struct DefaultDataStateItem {
    key: String,
    value: String,
}

impl DefaultDataStateItem {
    pub fn new(key: String, value: String) -> Self {
        Self { key, value }
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

impl DataStateItem for DefaultDataStateItem {}
