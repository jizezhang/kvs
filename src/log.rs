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

pub struct ValueEntry {
    pub file_id: String,
    pub vsz: usize,
    pub vpos: u64,
}

pub struct Wal {
    dir: PathBuf,
    current: File,
    current_filename: String,
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
        let current = Wal::open_log_file(&log_dir.join(&filename))?;
        Ok(Wal {
            dir: log_dir,
            current,
            current_filename: filename,
        })
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

    fn open_log_file(path: &Path) -> Result<File> {
        let file_handle = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok(file_handle)
    }

    pub fn replay(&mut self, map: &mut HashMap<String, ValueEntry>) -> Result<()> {
        let files = Wal::search_log_files(&self.dir)?;
        for file in files {
            self.current = Wal::open_log_file(&self.dir.join(&file))?;
            self.load_log_file(file, map)?;
        }
        Ok(())
    }

    fn load_log_file(
        &mut self,
        file_id: String,
        map: &mut HashMap<String, ValueEntry>,
    ) -> Result<()> {
        let mut offset: u64 = 0;
        loop {
            let result = self.read(&mut offset);
            match result {
                Ok((key, size)) => {
                    if size == 0 {
                        map.remove(&key);
                    } else {
                        map.insert(
                            key,
                            ValueEntry {
                                file_id: file_id.clone(),
                                vsz: size,
                                vpos: offset,
                            },
                        );
                        offset += size as u64; // skip reading value
                    }
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

    fn create_log_file_if_needed(&mut self) -> Result<()> {
        if self.current.metadata()?.len() >= Wal::SEGMENT_SIZE {
            self.current = Wal::open_log_file(&self.dir.join(Wal::generate_log_file_name()))?;
        }
        Ok(())
    }

    pub fn write(&mut self, cmd: Cmd) -> Result<ValueEntry> {
        self.create_log_file_if_needed()?;

        let key = cmd.key;
        let ksz = key.len();
        let val = cmd.value;
        let vsz = match cmd.operation {
            Operation::SET => val.len(),
            Operation::RM => 0,
        };

        let ksz_buf = ksz.to_ne_bytes();
        self.current.write_all(&ksz_buf)?;
        let vsz_buf = vsz.to_ne_bytes();
        self.current.write_all(&vsz_buf)?;

        let key_buf = key.as_bytes();
        self.current.write_all(key_buf)?;
        let vpos = self.current.metadata()?.len();
        if vsz > 0 {
            let val_buf = val.as_bytes();
            self.current.write_all(val_buf)?;
        }
        Ok(ValueEntry {
            file_id: self.current_filename.clone(),
            vsz,
            vpos,
        })
    }

    fn read(&mut self, offset: &mut u64) -> Result<(String, usize)> {
        let ksz = self.read_size(offset)?;
        let vsz = self.read_size(offset)?;

        let mut key_buf = vec![0u8; ksz];
        self.current.read_exact_at(&mut key_buf, *offset)?;
        *offset += ksz as u64;
        let key = String::from_utf8(key_buf)?;
        Ok((key, vsz))
    }

    fn read_size(&mut self, offset: &mut u64) -> Result<usize> {
        let mut buf = [0u8; std::mem::size_of::<usize>()];
        self.current.read_exact_at(&mut buf, *offset)?;
        let size = usize::from_ne_bytes(buf);
        *offset += std::mem::size_of::<usize>() as u64;
        Ok(size)
    }

    pub fn read_value(&mut self, ve: &ValueEntry) -> Result<String> {
        let mut buf = vec![0u8; ve.vsz];
        let file = Wal::open_log_file(&self.dir.join(&ve.file_id))?;
        file.read_exact_at(&mut buf, ve.vpos)?;
        Ok(String::from_utf8(buf)?)
    }
}
