use dashmap::DashMap;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::time;

use crate::Result;

#[derive(Debug, Clone)]
struct ValueEntry {
    value: String,
    expires_at: Option<SystemTime>,
}

#[derive(Debug, Clone)]
pub struct Storage {
    data: Arc<DashMap<String, ValueEntry>>,
}

impl Storage {
    pub fn new() -> Self {
        let storage = Self {
            data: Arc::new(DashMap::new()),
        };

        let data = storage.data.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(1));

            loop {
                interval.tick().await;
                Storage::cleanup_expired_keys(&data);
            }
        });

        storage
    }

    pub fn set(&self, key: String, value: String, expiry: Option<Duration>) -> Result<()> {
        let expires_at = expiry.map(|duration| SystemTime::now() + duration);
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
}
