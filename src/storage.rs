use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Write},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;
use tokio::time;
use tracing::error;

use crate::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ValueEntry {
    value: String,
    expires_at: Option<SystemTime>,
}

#[derive(Debug, Serialize, Deserialize)]
enum Operation {
    Set {
        key: String,
        value: String,
        expires_at: Option<SystemTime>,
    },
    Delete {
        key: String,
    },
}

#[derive(Debug, Clone)]
pub struct Storage {
    data: Arc<DashMap<String, ValueEntry>>,
    aof_writer: Arc<RwLock<BufWriter<File>>>,
    rdb_path: PathBuf,
    aof_path: PathBuf,
}

impl Storage {
    pub fn new() -> Result<Self> {
        let rdb_path = PathBuf::from("dump.rdb");
        let aof_path = PathBuf::from("appendonly.aof");
        let aof_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&aof_path)?;

        let aof_writer = Arc::new(RwLock::new(BufWriter::new(aof_file)));
        let data = Arc::new(DashMap::new());

        let storage = Self {
            data,
            aof_writer,
            rdb_path,
            aof_path,
        };

        storage.load_persistent_data()?;
        storage.start_background_tasks();

        Ok(storage)
    }

    fn start_background_tasks(&self) {
        let data = self.data.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                Storage::cleanup_expired_keys(&data);
            }
        });

        let storage = self.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(900));
            loop {
                interval.tick().await;
                if let Err(e) = storage.save_rdb().await {
                    error!("Failed to save RDB: {}", e);
                }
            }
        });

        let storage = self.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(3600));
            loop {
                interval.tick().await;
                if let Err(e) = storage.compact_aof().await {
                    error!("Failed to compact AOF: {}", e);
                }
            }
        });

        let writer = self.aof_writer.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let mut guard = writer.write().await;
                if let Err(e) = guard.flush() {
                    error!("Failed to flush AOF: {}", e);
                }
            }
        });
    }

    async fn save_rdb(&self) -> io::Result<()> {
        let temp_path = self.rdb_path.with_extension("temp");
        let file = File::create(&temp_path)?;
        let mut writer = BufWriter::new(file);

        let snapshot: Vec<_> = self
            .data
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        serde_json::to_writer(&mut writer, &snapshot)?;
        writer.flush()?;

        std::fs::rename(temp_path, &self.rdb_path)?;
        Ok(())
    }

    fn load_persistent_data(&self) -> Result<()> {
        if self.rdb_path.exists() {
            let file = File::open(&self.rdb_path)?;
            let reader = BufReader::new(file);
            let snapshot: Vec<(String, ValueEntry)> = serde_json::from_reader(reader).unwrap();

            for (key, value) in snapshot {
                if value
                    .expires_at
                    .map_or(true, |expires| SystemTime::now() <= expires)
                {
                    self.data.insert(key, value);
                }
            }
        }

        if self.aof_path.exists() {
            let file = File::open(&self.aof_path)?;
            let reader = BufReader::new(file);

            for line in io::BufRead::lines(reader) {
                let line = line?;
                if let Ok(op) = serde_json::from_str::<Operation>(&line) {
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
            }
        }

        Ok(())
    }

    pub fn set(&self, key: String, value: String, expiry: Option<Duration>) -> Result<()> {
        let expires_at = expiry.map(|duration| SystemTime::now() + duration);
        let op = Operation::Set {
            key: key.clone(),
            value: value.clone(),
            expires_at,
        };

        let entry = serde_json::to_string(&op).unwrap();

        if let Ok(mut writer) = self.aof_writer.try_write() {
            writeln!(writer, "{}", entry)?;
        }

        self.data.remove(&key);
        self.data.insert(key, ValueEntry { value, expires_at });
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<String>> {
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

    fn cleanup_expired_keys(data: &DashMap<String, ValueEntry>) {
        let now = SystemTime::now();
        data.retain(|_, entry| {
            if let Some(expires_at) = entry.expires_at {
                now <= expires_at
            } else {
                true
            }
        });
    }

    pub async fn compact_aof(&self) -> io::Result<()> {
        let temp_path = self.aof_path.with_extension("temp");
        let mut writer = BufWriter::new(File::create(&temp_path)?);

        for entry in self.data.iter() {
            let op = Operation::Set {
                key: entry.key().clone(),
                value: entry.value().value.clone(),
                expires_at: entry.value().expires_at,
            };
            writeln!(writer, "{}", serde_json::to_string(&op)?)?;
        }

        writer.flush()?;
        std::fs::rename(temp_path, &self.aof_path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn basic_set_get() -> Result<()> {
        let storage = Storage::new()?;

        storage.set("key1".to_string(), "value1".to_string(), None)?;
        let value = storage.get("key1")?;

        assert_eq!(value, Some("value1".to_string()));
        assert_eq!(storage.get("nonexistent")?, None);

        Ok(())
    }

    #[tokio::test]
    async fn expiry() -> Result<()> {
        let storage = Storage::new()?;

        storage.set(
            "expire_key".to_string(),
            "expire_value".to_string(),
            Some(Duration::from_secs(1)),
        )?;

        assert_eq!(storage.get("expire_key")?, Some("expire_value".to_string()));

        tokio::time::sleep(Duration::from_secs(2)).await;

        assert_eq!(storage.get("expire_key")?, None);

        Ok(())
    }

    #[tokio::test]
    async fn persistence() -> Result<()> {
        {
            let storage = Storage::new()?;
            storage.set("persist1".to_string(), "value1".to_string(), None)?;
            storage.set("persist2".to_string(), "value2".to_string(), None)?;

            tokio::time::sleep(Duration::from_secs(2)).await;
        }

        let storage2 = Storage::new()?;

        assert_eq!(storage2.get("persist1")?, Some("value1".to_string()));
        assert_eq!(storage2.get("persist2")?, Some("value2".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn overwrite() -> Result<()> {
        let storage = Storage::new()?;

        storage.set("key1".to_string(), "value1".to_string(), None)?;
        storage.set("key1".to_string(), "value2".to_string(), None)?;

        assert_eq!(storage.get("key1")?, Some("value2".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn cleanup_expired_keys() -> Result<()> {
        let storage = Storage::new()?;

        for i in 0..5 {
            storage.set(
                format!("expire_key{}", i),
                format!("value{}", i),
                Some(Duration::from_millis(100)),
            )?;
        }

        for i in 0..5 {
            storage.set(format!("perm_key{}", i), format!("value{}", i), None)?;
        }

        tokio::time::sleep(Duration::from_secs(1)).await;

        for i in 0..5 {
            assert_eq!(storage.get(&format!("expire_key{}", i))?, None);
        }

        for i in 0..5 {
            assert_eq!(
                storage.get(&format!("perm_key{}", i))?,
                Some(format!("value{}", i))
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn concurrent_access() -> Result<()> {
        let storage = Arc::new(Storage::new()?);
        let mut handles = vec![];

        for i in 0..10 {
            let storage_clone = storage.clone();
            handles.push(tokio::spawn(async move {
                storage_clone
                    .set(format!("concurrent_key{}", i), format!("value{}", i), None)
                    .unwrap();
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        for i in 0..10 {
            assert_eq!(
                storage.get(&format!("concurrent_key{}", i))?,
                Some(format!("value{}", i))
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn rdb_save_and_load() -> Result<()> {
        let temp_rdb = "dump_test.rdb";
        let temp_aof = "appendonly_test.aof";

        {
            let _ = std::fs::remove_file(temp_rdb);
            let _ = std::fs::remove_file(temp_aof);

            let storage = Storage::new()?;

            for i in 0..10 {
                storage.set(format!("key{}", i), format!("value{}", i), None)?;
            }

            storage.save_rdb().await?;

            if let Ok(mut writer) = storage.aof_writer.try_write() {
                writer.flush()?;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        let storage2 = Storage::new()?;

        for i in 0..10 {
            let key = format!("key{}", i);
            let expected = format!("value{}", i);
            let actual = storage2
                .get(&key)?
                .expect(&format!("Key {} should exist", key));
            assert_eq!(actual, expected, "Value mismatch for key {}", key);
        }

        let _ = std::fs::remove_file(temp_rdb);
        let _ = std::fs::remove_file(temp_aof);

        Ok(())
    }
}
