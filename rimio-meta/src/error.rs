use thiserror::Error;

pub type Result<T> = std::result::Result<T, MetaError>;

#[derive(Debug, Error)]
pub enum MetaError {
    #[error("config error: {0}")]
    Config(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("internal error: {0}")]
    Internal(String),
}
