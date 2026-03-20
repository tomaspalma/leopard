pub mod item;
pub mod state;

use async_trait::async_trait;

use item::{DataStateItem, DefaultDataStateItem};

use dashmap::DashMap;

use serde::{Serialize, de::DeserializeOwned};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

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
}

pub struct KeyValueDataStateStorage {
    memory_storage: DashMap<String, String>,
    persistent_storage: PersistentDataStorage,
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
        }
    }
}

#[async_trait]
impl DataStateStorage for KeyValueDataStateStorage {
    async fn save(&self, item: Box<dyn DataStateItem + Send + Sync>) {
        self.memory_storage
            .insert(item.key().to_string(), item.value().to_string());

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
                    let item: Box<dyn DataStateItem + Send + Sync> = Box::new(DefaultDataStateItem::new(key.to_string(), val.to_string()));
                    item
                });
            }
        }

        None
    }
}
