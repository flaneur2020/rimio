use thiserror::Error;

pub type Result<T> = std::result::Result<T, MetaError>;

#[derive(Debug, Error)]
pub enum MetaError {
    #[error("config error: {0}")]
    Config(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("http error: {0}")]
    Http(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("internal error: {0}")]
    Internal(String),
}

impl From<reqwest::Error> for MetaError {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value.to_string())
    }
}
