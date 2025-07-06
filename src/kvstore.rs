use std::collections::HashMap;
use std::path::Path;

use crate::error::{KvsError, Result};
use crate::log::{Cmd, Operation, ValueEntry, Wal};

pub struct KvStore {
    map: HashMap<String, ValueEntry>,
    log: Wal,
    ops_count: u64,
}

impl KvStore {
    const COMPACTION_THRESHOLD: f32 = 0.7;

    pub fn open(path: &Path) -> Result<KvStore> {
        let mut kvstore = KvStore {
            map: HashMap::new(),
            log: Wal::open(path)?,
            ops_count: 0,
        };
        kvstore.log.replay(&mut kvstore.map)?;
        Ok(kvstore)
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.map.get(&key) {
            Some(ve) => {
                if ve.vsz > 0 {
                    Ok(Some(self.log.read_value(ve)?))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Cmd {
            operation: Operation::SET,
            key: key.clone(),
            value: value.clone(),
        };
        let ve = self.log.write(cmd)?;
        self.map.insert(key, ve);
        self.compact_if_needed()?;
        self.ops_count += 1;
        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.map.remove(&key).is_some() {
            let cmd = Cmd {
                operation: Operation::RM,
                key,
                value: String::from(""),
            };
            self.log.write(cmd)?;
            self.ops_count += 1;
            Ok(())
        } else {
            Err(KvsError::KeyNotFound(key))
        }
    }

    fn compact_if_needed(&mut self) -> Result<()> {
        if self.map.len() as f32 / self.ops_count as f32 <= KvStore::COMPACTION_THRESHOLD {
            self.log.compact(&mut self.map)?;
        }
        Ok(())
    }
}
