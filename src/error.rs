use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum KvsError {
    #[error("I/O error: {0}")]
    IoError(#[from] io::Error),

    #[error("SerDe error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("End of file")]
    EofError,

    #[error("Key {0} not found")]
    KeyNotFound(String),
}

pub type Result<T> = std::result::Result<T, KvsError>;
