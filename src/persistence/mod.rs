use std::time::SystemTime;

use serde::{Deserialize, Serialize};

pub mod aof;
pub mod rdb;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueEntry {
    pub value: String,
    pub expires_at: Option<SystemTime>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Operation {
    Set {
        key: String,
        value: String,
        expires_at: Option<SystemTime>,
    },
    Delete {
        key: String,
    },
}
