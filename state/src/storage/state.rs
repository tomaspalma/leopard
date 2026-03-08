use crate::storage::{
    DataStateStorage, KeyValueDataStateStorage,
    item::{DataStateItem, DefaultDataStateItem},
};

pub trait DataState {
    type Item: DataStateItem;
    type Storage: DataStateStorage;

    fn store(&self, item: Box<Self::Item>);
    fn get(&self, key: &str) -> Option<Box<Self::Item>>;
    fn items(&self) -> Vec<Box<Self::Item>>;
}

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

impl DataState for DefaultDataState {
    type Item = DefaultDataStateItem;
    type Storage = KeyValueDataStateStorage;

    fn store(&self, _item: Box<Self::Item>) {
        self.storage.save(_item);
    }

    fn get(&self, _key: &str) -> Option<Box<Self::Item>> {
        self.storage.get(_key)
    }

    fn items(&self) -> Vec<Box<Self::Item>> {
        vec![]
    }
}
