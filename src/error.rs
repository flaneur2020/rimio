use thiserror::Error;

pub type Result<T> = std::result::Result<T, AmberError>;

#[derive(Error, Debug)]
pub enum AmberError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("Etcd error: {0}")]
    Etcd(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Slot not found: {0}")]
    SlotNotFound(u16),

    #[error("Chunk not found: {0}")]
    ChunkNotFound(String),

    #[error("Object not found: {0}")]
    ObjectNotFound(String),

    #[error("Insufficient replicas: need {required}, found {found}")]
    InsufficientReplicas { required: usize, found: usize },

    #[error("2PC failed: {0}")]
    TwoPhaseCommit(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("HTTP error: {0}")]
    Http(String),

    #[error("Content hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<etcd_client::Error> for AmberError {
    fn from(err: etcd_client::Error) -> Self {
        AmberError::Etcd(err.to_string())
    }
}
