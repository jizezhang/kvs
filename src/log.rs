use std::{
    collections::HashMap,
    fs::{File, OpenOptions, create_dir_all, read_dir, remove_file},
    io::{ErrorKind, Write},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};
use uuid::Uuid;

use crate::error::{KvsError, Result};

#[derive(Debug)]
pub enum Operation {
    SET,
    RM,
}

pub struct ValueEntry {
    pub file_id: Box<String>,
    pub vsz: usize,
    pub vpos: u64,
}

pub struct Wal {
    dir: PathBuf,
    files: Vec<Box<String>>,
}

impl Wal {
    const SEGMENT_SIZE: u64 = 128;

    pub fn open(path: &Path) -> Result<Wal> {
        let dir = path.join(".log");
        create_dir_all(&dir)?;
        let files = Wal::search_log_files(&dir)?;
        Ok(Wal { dir, files })
    }

    pub fn search_log_files(path: &Path) -> Result<Vec<Box<String>>> {
        let entries = read_dir(path)?;
        let mut file_names: Vec<Box<String>> = entries
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().into_string().ok())
            .filter(|name| name.ends_with(".log"))
            .map(Box::new)
            .collect();
        file_names.sort();
        Ok(file_names)
    }

    fn generate_log_file_name() -> String {
        let log_id = Uuid::now_v7();
        log_id.to_string() + ".log"
    }

    fn open_log_file(&self, fname: &String) -> Result<File> {
        let file_handle = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(self.dir.join(Path::new(fname)))?;
        Ok(file_handle)
    }

    fn open_file(path: PathBuf) -> Result<File> {
        let file_handle = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        Ok(file_handle)
    }

    pub fn replay(&self, map: &mut HashMap<String, ValueEntry>) -> Result<u64> {
        let mut total_entries = 0;
        for file in &self.files {
            total_entries += self.load_log_file(file.clone(), map)?;
        }
        Ok(total_entries)
    }

    pub fn compact(&mut self, map: &mut HashMap<String, ValueEntry>) -> Result<()> {
        let before = self.files.len();
        self.files.push(Box::new(Wal::generate_log_file_name()));
        for (k, ve) in map {
            let value = self.read_value(ve)?;
            let nve = self.write(k, &value, Operation::SET)?;
            *ve = nve;
        }
        for _ in 0..before {
            remove_file(self.dir.join(*self.files.remove(0)))?;
        }
        Ok(())
    }

    fn load_log_file(
        &self,
        file_id: Box<String>,
        map: &mut HashMap<String, ValueEntry>,
    ) -> Result<u64> {
        let mut offset: u64 = 0;
        let file_ref = &*file_id;
        let mut entry_count = 0;
        loop {
            let result = self.read(&mut offset, file_ref);
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
                    entry_count += 1;
                }
                Err(err) => match &err {
                    KvsError::IoError(error) => {
                        if error.kind() == ErrorKind::UnexpectedEof {
                            return Ok(entry_count);
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
        match self.files.last() {
            Some(f) => {
                let current = self.open_log_file(f)?;
                if current.metadata()?.len() >= Wal::SEGMENT_SIZE {
                    self.files.push(Box::new(Wal::generate_log_file_name()));
                }
            }
            None => {
                self.files.push(Box::new(Wal::generate_log_file_name()));
            }
        }
        Ok(())
    }

    pub fn write(&mut self, k: &String, v: &String, mode: Operation) -> Result<ValueEntry> {
        self.create_log_file_if_needed()?;
        let mut current = self.open_log_file(self.files.last().unwrap())?;

        let ksz = (*k).len();
        let vsz = match mode {
            Operation::SET => (*v).len(),
            Operation::RM => 0,
        };

        let ksz_buf = ksz.to_ne_bytes();
        current.write_all(&ksz_buf)?;
        let vsz_buf = vsz.to_ne_bytes();
        current.write_all(&vsz_buf)?;

        let key_buf = (*k).as_bytes();
        current.write_all(key_buf)?;
        let vpos = current.metadata()?.len();
        if vsz > 0 {
            let val_buf = (*v).as_bytes();
            current.write_all(val_buf)?;
        }
        Ok(ValueEntry {
            file_id: self.files.last().unwrap().clone(),
            vsz,
            vpos,
        })
    }

    fn read(&self, offset: &mut u64, file_id: &String) -> Result<(String, usize)> {
        let ksz = self.read_size(offset, file_id)?;
        let vsz = self.read_size(offset, file_id)?;

        let mut key_buf = vec![0u8; ksz];
        let current = self.open_log_file(file_id)?;
        current.read_exact_at(&mut key_buf, *offset)?;
        *offset += ksz as u64;
        let key = String::from_utf8(key_buf)?;
        Ok((key, vsz))
    }

    fn read_size(&self, offset: &mut u64, file_id: &String) -> Result<usize> {
        let mut buf = [0u8; std::mem::size_of::<usize>()];
        let current = self.open_log_file(file_id)?;
        current.read_exact_at(&mut buf, *offset)?;
        let size = usize::from_ne_bytes(buf);
        *offset += std::mem::size_of::<usize>() as u64;
        Ok(size)
    }

    pub fn read_value(&self, ve: &ValueEntry) -> Result<String> {
        let mut buf = vec![0u8; ve.vsz];
        let file = Wal::open_file(self.dir.join(Path::new(&*ve.file_id)))?;
        file.read_exact_at(&mut buf, ve.vpos)?;
        Ok(String::from_utf8(buf)?)
    }
}
