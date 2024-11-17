use std::{
    fs, io,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};

use dashmap::DashMap;
use tracing::{error, info};

use crate::persistence::{aof::AofManager, rdb::RdbManager, Operation, ValueEntry};

pub struct Storage {
    data: Arc<DashMap<String, ValueEntry>>,
    aof_manager: Arc<AofManager>,
    rdb_manager: Arc<RdbManager>,
}

impl Storage {
    pub fn new() -> io::Result<Self> {
        Self::new_with_paths(
            PathBuf::from("data/dump.rdb"),
            PathBuf::from("data/appendonly.aof"),
        )
    }

    pub fn new_with_paths(rdb_path: PathBuf, aof_path: PathBuf) -> io::Result<Self> {
        fs::create_dir_all("data")?;

        info!(
            "Initializing storage with RDB: {:?}, AOF: {:?}",
            rdb_path, aof_path
        );

        let aof_manager = Arc::new(AofManager::new(aof_path)?);
        let rdb_manager = Arc::new(RdbManager::new(rdb_path));
        let data = Arc::new(DashMap::new());

        let storage = Self {
            data,
            aof_manager,
            rdb_manager,
        };

        storage.load_persistent_data()?;
        storage.start_background_tasks();

        Ok(storage)
    }

    fn load_persistent_data(&self) -> io::Result<()> {
        for (key, value) in self.rdb_manager.load()? {
            if value
                .expires_at
                .map_or(true, |expires| SystemTime::now() <= expires)
            {
                self.data.insert(key, value);
            }
        }

        for op in self.aof_manager.load_operations()? {
            match op {
                Operation::Set {
                    key,
                    value,
                    expires_at,
                } => {
                    if expires_at.map_or(true, |expires| SystemTime::now() <= expires) {
                        self.data.insert(key, ValueEntry { value, expires_at });
                    }
                }
                Operation::Delete { key } => {
                    self.data.remove(&key);
                }
            }
        }

        Ok(())
    }

    fn start_background_tasks(&self) {
        let storage_clone = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300));
            loop {
                interval.tick().await;
                let snapshot: Vec<_> = storage_clone
                    .data
                    .iter()
                    .map(|entry| (entry.key().clone(), entry.value().clone()))
                    .collect();
                if let Err(e) = storage_clone.rdb_manager.save(&snapshot).await {
                    error!("Failed to save RDB: {}", e);
                }
            }
        });

        let storage_clone = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                if let Err(e) = storage_clone.aof_manager.sync().await {
                    error!("Failed to sync AOF: {}", e);
                }
            }
        });

        let storage_clone = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3600));
            loop {
                interval.tick().await;
                let snapshot: Vec<_> = storage_clone
                    .data
                    .iter()
                    .map(|entry| (entry.key().clone(), entry.value().clone()))
                    .collect();
                if let Err(e) = storage_clone.aof_manager.compact(&snapshot).await {
                    error!("Failed to compact AOF: {}", e);
                }
            }
        });
    }

    pub async fn set(
        &self,
        key: String,
        value: String,
        expiry: Option<Duration>,
    ) -> io::Result<()> {
        let expires_at = expiry.map(|duration| SystemTime::now() + duration);
        let op = Operation::Set {
            key: key.clone(),
            value: value.clone(),
            expires_at,
        };

        self.aof_manager.append_operation(&op).await?;
        self.data.insert(key, ValueEntry { value, expires_at });

        Ok(())
    }

    pub async fn get(&self, key: &str) -> io::Result<Option<String>> {
        if let Some(ref_multi) = self.data.get(key) {
            if let Some(expires_at) = ref_multi.expires_at {
                if SystemTime::now() > expires_at {
                    drop(ref_multi);
                    self.data.remove(key);
                    return Ok(None);
                }
            }
            Ok(Some(ref_multi.value.clone()))
        } else {
            Ok(None)
        }
    }
}

impl Clone for Storage {
    fn clone(&self) -> Self {
        Self {
            data: Arc::clone(&self.data),
            aof_manager: Arc::clone(&self.aof_manager),
            rdb_manager: Arc::clone(&self.rdb_manager),
        }
    }
}
