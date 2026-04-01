pub mod item;
pub mod state;

use tracing::info;

use async_trait::async_trait;

use item::{DataStateItem, DefaultDataStateItem};

use dashmap::DashMap;

use serde::{Serialize, de::DeserializeOwned};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageAction {
    Insert,
}

pub type StorageListener = Box<dyn Fn(&dyn DataStateItem) + Send + Sync>;

pub struct PersistentDataStorage {
    filename: String,
}

impl PersistentDataStorage {
    pub fn new(filename: String) -> Self {
        Self {
            filename: filename.to_string(),
        }
    }

    pub async fn save<T: Serialize>(&self, data: &T) -> std::io::Result<()> {
        let file = File::create(&self.filename)?;
        let writer = BufWriter::new(file);

        serde_json::to_writer_pretty(writer, data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(())
    }

    pub async fn load<T: DeserializeOwned>(&self) -> std::io::Result<T> {
        let file = File::open(&self.filename)?;
        let reader = BufReader::new(file);

        let data = serde_json::from_reader(reader)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(data)
    }

    pub fn exists(&self) -> bool {
        Path::new(&self.filename).exists()
    }
}

#[async_trait]
pub trait DataStateStorage {
    async fn save(&self, item: Box<dyn DataStateItem + Send + Sync>);
    async fn get(&self, key: &str) -> Option<Box<dyn DataStateItem + Send + Sync>>;
    fn items(&self) -> Vec<Box<dyn DataStateItem + Send + Sync>>;
    fn add_listener(&self, action: StorageAction, listener: StorageListener);
}

pub struct KeyValueDataStateStorage {
    memory_storage: DashMap<String, String>,
    persistent_storage: PersistentDataStorage,
    listeners: DashMap<StorageAction, Vec<StorageListener>>,
}

impl KeyValueDataStateStorage {
    pub async fn new(persistent_filename: Option<String>) -> Self {
        let persistent_storage =
            PersistentDataStorage::new(if let Some(filename) = persistent_filename {
                filename.to_string()
            } else {
                "data.json".to_string()
            });
        let memory_storage = DashMap::new();

        if persistent_storage.exists() {
            if let Ok(data) = persistent_storage.load::<DashMap<String, String>>().await {
                for (k, v) in data {
                    memory_storage.insert(k, v);
                }
            }
        }

        Self {
            memory_storage,
            persistent_storage,
            listeners: DashMap::new(),
        }
    }
}

#[async_trait]
impl DataStateStorage for KeyValueDataStateStorage {
    fn add_listener(&self, action: StorageAction, listener: StorageListener) {
        self.listeners.entry(action).or_default().push(listener);
    }

    async fn save(&self, item: Box<dyn DataStateItem + Send + Sync>) {
        info!("Saving item {}:{}", item.key(), item.value());

        let key = item.key().to_string();
        let value = item.value().to_string();

        self.memory_storage.insert(key, value);

        let action = StorageAction::Insert;
        if let Some(action_listeners) = self.listeners.get(&action) {
            for listener in action_listeners.iter() {
                listener(item.as_ref());
            }
        }

        self.persistent_storage
            .save(&self.memory_storage)
            .await
            .unwrap();
    }

    async fn get(&self, key: &str) -> Option<Box<dyn DataStateItem + Send + Sync>> {
        if let Some(val) = self.memory_storage.get(key) {
            return Some(Box::new(DefaultDataStateItem::new(
                key.to_string(),
                val.to_string(),
            )));
        }

        if self.persistent_storage.exists() {
            if let Ok(disk_map) = self
                .persistent_storage
                .load::<DashMap<String, String>>()
                .await
            {
                for entry in disk_map.iter() {
                    self.memory_storage
                        .insert(entry.key().clone(), entry.value().clone());
                }

                return self.memory_storage.get(key).map(|val| {
                    let item: Box<dyn DataStateItem + Send + Sync> =
                        Box::new(DefaultDataStateItem::new(key.to_string(), val.to_string()));
                    item
                });
            }
        }

        None
    }

    fn items(&self) -> Vec<Box<dyn DataStateItem + Send + Sync>> {
        self.memory_storage
            .iter()
            .map(|entry| {
                Box::new(DefaultDataStateItem::new(
                    entry.key().clone(),
                    entry.value().clone(),
                )) as Box<dyn DataStateItem + Send + Sync>
            })
            .collect()
    }
}
