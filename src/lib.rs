//! ChunkDB - Coordinated Storage Engine
//!
//! A next-generation HTAP database with Zarr-style chunking and direct query execution.

mod error;
pub use error::{ChunkDbError, Result};

pub mod config;
pub use config::TableConfig;

pub mod storage;
pub use storage::{ChunkCoordinate, ChunkInfo};

pub mod catalog;
pub use catalog::{VersionCatalog, HashRegistry};

pub mod partitioning;

pub mod write;
pub use write::BatchInserter;

pub mod query;
pub use query::{Filter, FilterOp, FilterValue, CompositeFilter, QueryBuilder};

pub mod api;
pub use api::{ChunkDb, TableBuilder};

// Re-export commonly used Arrow types
pub use arrow::array::RecordBatch;


