use async_trait::async_trait;

use crate::storage::{
    DataStateStorage, KeyValueDataStateStorage,
    item::{DataStateItem, DefaultDataStateItem},
};

#[async_trait]
pub trait DataState {
    type Item: DataStateItem;
    type Storage: DataStateStorage;

    async fn store(&self, item: Box<Self::Item>);
    async fn get(&self, key: &str) -> Option<Box<Self::Item>>;
    fn items(&self) -> Vec<Box<Self::Item>>;
}

pub struct DefaultDataState {
    storage: KeyValueDataStateStorage,
}

impl DefaultDataState {
    pub async fn new(persistent_filename: String) -> Self {
        Self {
            storage: KeyValueDataStateStorage::new(Some(persistent_filename)).await,
        }
    }
}

#[async_trait]
impl DataState for DefaultDataState {
    type Item = DefaultDataStateItem;
    type Storage = KeyValueDataStateStorage;

    async fn store(&self, _item: Box<Self::Item>) {
        self.storage.save(_item).await;
    }

    async fn get(&self, _key: &str) -> Option<Box<Self::Item>> {
        self.storage.get(_key).await
    }

    fn items(&self) -> Vec<Box<Self::Item>> {
        vec![]
    }
}
