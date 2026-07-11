use serde::{Deserialize, Serialize};

/// Represents the multi-dimensional coordinate of a chunk
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkCoordinate {
    /// Row bucket index at `level`: bucket_at_level(row_id, chunk_rows, level).
    /// At level 0 this is row_id // chunk_rows (v0 behavior).
    pub row_bucket: u64,

    /// Refinement level of the row dimension (0 = base grid).
    /// Cell (level, b) splits into (level+1, 2b) and (level+1, 2b+1).
    pub level: u16,

    /// Column group index
    pub col_group: u16,

    /// Hash bucket indices (one per hash dimension, in order)
    pub hash_buckets: Vec<u64>,

    /// Range bucket indices (one per range dimension, in order)
    pub range_buckets: Vec<u64>,
}

impl ChunkCoordinate {
    /// Coordinate at base level 0 (fixed-grid / v0 semantics)
    pub fn new(
        row_bucket: u64,
        col_group: u16,
        hash_buckets: Vec<u64>,
        range_buckets: Vec<u64>,
    ) -> Self {
        Self::new_at_level(row_bucket, 0, col_group, hash_buckets, range_buckets)
    }

    pub fn new_at_level(
        row_bucket: u64,
        level: u16,
        col_group: u16,
        hash_buckets: Vec<u64>,
        range_buckets: Vec<u64>,
    ) -> Self {
        Self {
            row_bucket,
            level,
            col_group,
            hash_buckets,
            range_buckets,
        }
    }

    /// Same coordinate re-addressed to a child row cell.
    pub fn child(&self, child_bucket: u64) -> Self {
        debug_assert!(child_bucket == 2 * self.row_bucket || child_bucket == 2 * self.row_bucket + 1);
        Self {
            row_bucket: child_bucket,
            level: self.level + 1,
            col_group: self.col_group,
            hash_buckets: self.hash_buckets.clone(),
            range_buckets: self.range_buckets.clone(),
        }
    }
}

/// Chunk with coordinate and version
#[derive(Debug, Clone)]
pub struct ChunkInfo {
    pub coord: ChunkCoordinate,
    pub version: u64,
}


