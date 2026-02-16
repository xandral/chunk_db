use thiserror::Error;

#[derive(Error, Debug)]
pub enum ChunkDbError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Catalog error: {0}")]
    Catalog(#[from] sled::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Chunk not found: {0}")]
    ChunkNotFound(String),

    #[error("Invalid chunk filename: {0}")]
    InvalidChunkFilename(String),

    #[error("Invalid query: {0}")]
    InvalidQuery(String),
}

pub type Result<T> = std::result::Result<T, ChunkDbError>;


