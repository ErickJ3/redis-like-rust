use bincode::{deserialize_from, serialize_into};

use super::ValueEntry;
use std::{
    fs::File,
    io::{self, BufReader, BufWriter},
    path::PathBuf,
};

pub struct RdbManager {
    path: PathBuf,
}

impl RdbManager {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub async fn save(&self, entries: &[(String, ValueEntry)]) -> io::Result<()> {
        let temp_path = self.path.with_extension("temp");
        let file = File::create(&temp_path)?;
        let writer = BufWriter::new(file);

        serialize_into(writer, &entries).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        std::fs::rename(temp_path, &self.path)?;
        Ok(())
    }

    pub fn load(&self) -> io::Result<Vec<(String, ValueEntry)>> {
        if self.path.exists() {
            let file = File::open(&self.path)?;
            let reader = BufReader::new(file);
            deserialize_from(reader).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
        } else {
            Ok(Vec::new())
        }
    }
}
