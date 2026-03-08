pub mod item;
pub mod state;

use item::{DataStateItem, DefaultDataStateItem};

use dashmap::DashMap;

pub trait DataStateStorage {
    type Item: DataStateItem;

    fn save(&self, item: Box<Self::Item>);
    fn get(&self, key: &str) -> Option<Box<Self::Item>>;
}

pub struct KeyValueDataStateStorage {
    storage: DashMap<String, String>,
}

impl KeyValueDataStateStorage {
    pub fn new() -> Self {
        Self {
            storage: DashMap::new(),
        }
    }
}

impl DataStateStorage for KeyValueDataStateStorage {
    type Item = DefaultDataStateItem;

    fn save(&self, item: Box<Self::Item>) {
        self.storage
            .insert(item.key().to_string(), item.value().to_string());
    }

    fn get(&self, key: &str) -> Option<Box<Self::Item>> {
        self.storage.get(key).map(|value| {
            Box::new(DefaultDataStateItem::new(
                key.to_string(),
                value.to_string(),
            ))
        })
    }
}
