use serde::{Deserialize, Serialize};

/// Represents the multi-dimensional coordinate of a chunk
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkCoordinate {
    /// Row bucket index (row_id // chunk_rows)
    pub row_bucket: u64,

    /// Column group index
    pub col_group: u16,

    /// Hash bucket indices (one per hash dimension, in order)
    pub hash_buckets: Vec<u64>,

    /// Range bucket indices (one per range dimension, in order)
    pub range_buckets: Vec<u64>,
}

impl ChunkCoordinate {
    pub fn new(
        row_bucket: u64,
        col_group: u16,
        hash_buckets: Vec<u64>,
        range_buckets: Vec<u64>,
    ) -> Self {
        Self {
            row_bucket,
            col_group,
            hash_buckets,
            range_buckets,
        }
    }
}

/// Chunk with coordinate and version
#[derive(Debug, Clone)]
pub struct ChunkInfo {
    pub coord: ChunkCoordinate,
    pub version: u64,
}


