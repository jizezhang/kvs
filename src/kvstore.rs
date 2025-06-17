use std::collections::HashMap;
use std::path::Path;

use crate::error::{KvsError, Result};
use crate::log::{Cmd, Operation, Wal};

pub struct KvStore {
    map: HashMap<String, String>,
    log: Wal,
}

impl KvStore {
    pub fn open(path: &Path) -> Result<KvStore> {
        let mut kvstore = KvStore {
            map: HashMap::new(),
            log: Wal::open(path)?,
        };
        kvstore.log.replay(&mut kvstore.map)?;
        Ok(kvstore)
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(self.map.get(&key).cloned())
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Cmd {
            operation: Operation::SET,
            key: key.clone(),
            value: value.clone(),
        };
        self.log.write(&cmd)?;
        self.map.insert(key, value);
        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.map.remove(&key).is_some() {
            let cmd = Cmd {
                operation: Operation::RM,
                key,
                value: String::from(""),
            };
            self.log.write(&cmd)?;
            Ok(())
        } else {
            Err(KvsError::KeyNotFound(key))
        }
    }
}
