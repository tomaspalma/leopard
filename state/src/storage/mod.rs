pub mod item;
pub mod state;

use runtime::spawn;

use tokio::fs::{read_to_string, write};
use tracing::info;

use async_trait::async_trait;

use item::{DataStateItem, DefaultDataStateItem};

use dashmap::DashMap;

use serde::{Serialize, de::DeserializeOwned};
use std::{path::Path, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageAction {
    Insert,
}

pub type StorageListener = Box<dyn Fn(&dyn DataStateItem) + Send + Sync>;

#[derive(Debug, Clone)]
pub struct PersistentDataStorage {
    filename: String,
    write_lock: std::sync::Arc<tokio::sync::Mutex<()>>,
}

impl PersistentDataStorage {
    pub fn new(filename: String) -> Self {
        Self {
            filename: filename.to_string(),
            write_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub async fn save<T: Serialize>(&self, data: &T) -> std::io::Result<()> {
        let _guard = self.write_lock.lock().await;

        let serialized = serde_json::to_string_pretty(data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let tmp_filename = format!("{}.tmp", self.filename);
        write(&tmp_filename, serialized).await?;
        tokio::fs::rename(&tmp_filename, &self.filename).await?;

        Ok(())
    }

    pub async fn load<T: DeserializeOwned>(&self) -> std::io::Result<T> {
        let content = read_to_string(&self.filename).await?;

        let data = serde_json::from_str(&content)
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
    fn keys(&self) -> Vec<String>;

    fn add_listener(&self, action: StorageAction, listener: StorageListener);
}

pub struct KeyValueDataStateStorage {
    memory_storage: Arc<DashMap<String, String>>,
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
            memory_storage: std::sync::Arc::new(memory_storage),
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

        let persistent_storage_clone = self.persistent_storage.clone();
        let memory_storage_clone = self.memory_storage.clone();
        spawn!({
            if let Err(e) = persistent_storage_clone
                .save(&*memory_storage_clone)
                .await
            {
                tracing::warn!("Failed to persist storage to disk: {}", e);
            }
        });
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

    fn keys(&self) -> Vec<String> {
        self.memory_storage
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }
}
