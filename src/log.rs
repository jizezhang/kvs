use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions, create_dir_all, read_dir},
    io::{ErrorKind, Write},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};
use uuid::Uuid;

use crate::error::{KvsError, Result};

#[derive(Serialize, Deserialize, Debug)]
pub enum Operation {
    SET,
    RM,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Cmd {
    pub operation: Operation,
    pub key: String,
    pub value: String,
}

pub struct Wal {
    dir: PathBuf,
    current: File,
}

impl Wal {
    const SEGMENT_SIZE: u64 = 1024;

    pub fn open(path: &Path) -> Result<Wal> {
        let log_dir = path.join(".log");
        create_dir_all(&log_dir)?;
        let filename = match Wal::search_log_files(&log_dir)?.last() {
            Some(name) => name.to_owned(),
            None => Wal::generate_log_file_name(),
        };
        let current = Wal::open_log_file(&log_dir.join(filename))?;
        Ok(Wal {
            dir: log_dir,
            current,
        })
    }

    pub fn replay(&mut self, map: &mut HashMap<String, String>) -> Result<()> {
        let files = Wal::search_log_files(&self.dir)?;
        for file in files {
            self.current = Wal::open_log_file(&self.dir.join(file))?;
            self.load_log_file(map)?;
        }
        Ok(())
    }

    fn load_log_file(&mut self, map: &mut HashMap<String, String>) -> Result<()> {
        let mut offset: u64 = 0;
        loop {
            let result = self.read(&mut offset);
            match result {
                Ok(cmd) => {
                    match cmd.operation {
                        Operation::SET => map.insert(cmd.key, cmd.value),
                        Operation::RM => map.remove(&cmd.key),
                    };
                }
                Err(err) => match &err {
                    KvsError::IoError(error) => {
                        if error.kind() == ErrorKind::UnexpectedEof {
                            return Ok(());
                        } else {
                            return Err(err);
                        }
                    }
                    _ => return Err(err),
                },
            }
        }
    }

    pub fn search_log_files(path: &Path) -> Result<Vec<String>> {
        let entries = read_dir(path)?;
        let mut file_names: Vec<String> = entries
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().into_string().ok())
            .filter(|name| name.ends_with(".log"))
            .collect();
        file_names.sort();
        Ok(file_names)
    }

    fn generate_log_file_name() -> String {
        let log_id = Uuid::now_v7();
        log_id.to_string() + ".log"
    }

    fn create_log_file_if_needed(&mut self) -> Result<()> {
        if self.current.metadata()?.len() >= Wal::SEGMENT_SIZE {
            self.current = Wal::create_log_file(&self.dir)?;
        }
        Ok(())
    }

    fn create_log_file(dir: &Path) -> Result<File> {
        Wal::open_log_file(&dir.join(Wal::generate_log_file_name()))
    }

    fn open_log_file(path: &Path) -> Result<File> {
        let file_handle = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok(file_handle)
    }

    pub fn write(&mut self, cmd: &Cmd) -> Result<u64> {
        let value = serde_json::to_string(cmd)?;
        let buf = value.as_bytes();
        let len_buf = buf.len().to_ne_bytes();

        self.create_log_file_if_needed()?;
        let start = self.current.metadata()?.len();
        self.current.write_all(&len_buf)?;
        self.current.write_all(buf)?;
        Ok(start)
    }

    pub fn read(&mut self, offset: &mut u64) -> Result<Cmd> {
        let mut buf = [0u8; std::mem::size_of::<usize>()];
        self.current.read_exact_at(&mut buf, *offset)?;
        let size = usize::from_ne_bytes(buf);
        let mut buf = vec![0u8; size];
        *offset += 8;
        self.current.read_exact_at(&mut buf, *offset)?;
        let cmd: Cmd = serde_json::from_slice(&buf)?;
        *offset += size as u64;
        Ok(cmd)
    }
}
