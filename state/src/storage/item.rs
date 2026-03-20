pub trait DataStateItem {
    fn key(&self) -> &str;
    fn value(&self) -> &str;
}

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

impl DataStateItem for DefaultDataStateItem {
    fn key(&self) -> &str {
        &self.key
    }

    fn value(&self) -> &str {
        &self.value
    }
}
