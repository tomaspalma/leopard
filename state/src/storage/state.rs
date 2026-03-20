use async_trait::async_trait;

use crate::storage::{
    DataStateStorage, KeyValueDataStateStorage,
    item::DataStateItem,
};

#[async_trait]
pub trait DataState {
    async fn store(&self, item: Box<dyn DataStateItem + Send + Sync>);
    async fn get(&self, key: &str) -> Option<Box<dyn DataStateItem + Send + Sync>>;
    fn items(&self) -> Vec<Box<dyn DataStateItem + Send + Sync>>;
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
    async fn store(&self, _item: Box<dyn DataStateItem + Send + Sync>) {
        self.storage.save(_item).await;
    }

    async fn get(&self, _key: &str) -> Option<Box<dyn DataStateItem + Send + Sync>> {
        self.storage.get(_key).await
    }

    fn items(&self) -> Vec<Box<dyn DataStateItem + Send + Sync>> {
        vec![]
    }
}
