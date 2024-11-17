use super::Operation;
use super::ValueEntry;
use bincode::{deserialize, serialize};
use std::io::Read;
use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Write},
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::{Mutex, RwLock};

pub struct AofManager {
    writer: RwLock<BufWriter<File>>,
    path: PathBuf,
    sync_counter: Arc<Mutex<usize>>,
}

impl AofManager {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(&path)?;

        let writer = RwLock::new(BufWriter::with_capacity(32 * 1024 * 1024, file));

        Ok(Self {
            writer,
            path,
            sync_counter: Arc::new(Mutex::new(0)),
        })
    }

    pub async fn append_operation(&self, op: &Operation) -> io::Result<()> {
        let serialized = serialize(op).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let len = (serialized.len() as u32).to_le_bytes();

        {
            let mut writer = self.writer.write().await;
            writer.write_all(&len)?;
            writer.write_all(&serialized)?;

            let mut counter = self.sync_counter.lock().await;
            *counter += 1;

            if *counter >= 1000 {
                writer.flush()?;
                writer.get_ref().sync_all()?;
                *counter = 0;
            }
        }

        Ok(())
    }

    pub async fn sync(&self) -> io::Result<()> {
        let mut writer = self.writer.write().await;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        Ok(())
    }

    pub async fn compact(&self, entries: &[(String, ValueEntry)]) -> io::Result<()> {
        let temp_path = self.path.with_extension("temp");

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)?;

        let mut writer = BufWriter::with_capacity(32 * 1024 * 1024, file);

        for (key, entry) in entries {
            let op = Operation::Set {
                key: key.clone(),
                value: entry.value.clone(),
                expires_at: entry.expires_at,
            };
            let serialized = serialize(&op).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            let len = (serialized.len() as u32).to_le_bytes();
            writer.write_all(&len)?;
            writer.write_all(&serialized)?;
        }

        writer.flush()?;
        writer.get_ref().sync_all()?;

        std::fs::rename(temp_path, &self.path)?;

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(&self.path)?;

        let mut writer = self.writer.write().await;
        *writer = BufWriter::with_capacity(32 * 1024 * 1024, file);

        Ok(())
    }

    pub fn load_operations(&self) -> io::Result<Vec<Operation>> {
        let mut operations = Vec::new();

        if self.path.exists() {
            let file = File::open(&self.path)?;
            let mut reader = BufReader::with_capacity(32 * 1024 * 1024, file);
            let mut len_bytes = [0u8; 4];

            while let Ok(_) = reader.read_exact(&mut len_bytes) {
                let len = u32::from_le_bytes(len_bytes) as usize;
                let mut buf = vec![0u8; len];
                reader.read_exact(&mut buf)?;

                match deserialize(&buf) {
                    Ok(op) => operations.push(op),
                    Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
                }
            }
        }

        Ok(operations)
    }
}
