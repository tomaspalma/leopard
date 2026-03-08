use std::collections::HashMap;

use dashmap::DashMap;

pub trait DataState {
    type Item: DataStateItem;
    type Storage: DataStateStorage;

    fn store(&mut self, item: Box<Self::Item>);
    fn get(&self, key: &str) -> Option<Box<Self::Item>>;
    fn items(&self) -> Vec<Box<Self::Item>>;
}

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
        self.storage.insert(item.key, item.value);
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

pub trait DataStateItem {}

pub struct DefaultDataState {
    storage: KeyValueDataStateStorage,
}

impl DefaultDataState {
    pub fn new() -> Self {
        Self {
            storage: KeyValueDataStateStorage::new(),
        }
    }
}

pub struct DefaultDataStateItem {
    key: String,
    value: String,
}

impl DefaultDataStateItem {
    pub fn new(key: String, value: String) -> Self {
        Self { key, value }
    }
}

impl DataStateItem for DefaultDataStateItem {}

impl DataState for DefaultDataState {
    type Item = DefaultDataStateItem;
    type Storage = KeyValueDataStateStorage;

    fn store(&mut self, _item: Box<Self::Item>) {
        self.storage.save(_item);
    }

    fn get(&self, _key: &str) -> Option<Box<Self::Item>> {
        self.storage.get(_key)
    }

    fn items(&self) -> Vec<Box<Self::Item>> {
        vec![]
    }
}
