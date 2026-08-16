use serde::{Deserialize, Serialize};

/// Coordinate shared by all physical column groups that contain the same rows.
///
/// A cell is a leaf of the adaptive grid. `row_level` is refined globally by
/// [`LevelMap`](crate::partitioning::LevelMap), while hash and range levels are
/// refined locally by [`DimensionMap`](crate::partitioning::DimensionMap).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CellCoordinate {
    pub row_bucket: u64,
    pub row_level: u16,
    pub hash_buckets: Vec<u64>,
    pub range_buckets: Vec<u64>,
    pub hash_levels: Vec<u16>,
    pub range_levels: Vec<u16>,
}

impl CellCoordinate {
    /// Construct an unrefined dimension cell inside an already-routed row
    /// leaf. Hash/range bucket vectors are level-0 buckets.
    pub fn base(
        row_bucket: u64,
        row_level: u16,
        hash_buckets: Vec<u64>,
        range_buckets: Vec<u64>,
    ) -> Self {
        Self {
            row_bucket,
            row_level,
            hash_levels: vec![0; hash_buckets.len()],
            range_levels: vec![0; range_buckets.len()],
            hash_buckets,
            range_buckets,
        }
    }

    /// Copy this dimension path into one child of a row-axis split.
    pub fn row_child(&self, child_bucket: u64) -> Self {
        debug_assert!(
            child_bucket == self.row_bucket.saturating_mul(2)
                || child_bucket == self.row_bucket.saturating_mul(2).saturating_add(1)
        );
        let mut child = self.clone();
        child.row_bucket = child_bucket;
        child.row_level += 1;
        child
    }

    /// Materialize the physical coordinate for one vertical column group.
    pub fn with_col_group(&self, col_group: u16) -> ChunkCoordinate {
        ChunkCoordinate {
            row_bucket: self.row_bucket,
            level: self.row_level,
            col_group,
            hash_buckets: self.hash_buckets.clone(),
            range_buckets: self.range_buckets.clone(),
            hash_levels: self.hash_levels.clone(),
            range_levels: self.range_levels.clone(),
        }
    }

    pub fn dimensions_are_consistent(&self) -> bool {
        self.hash_buckets.len() == self.hash_levels.len()
            && self.range_buckets.len() == self.range_levels.len()
    }
}

/// Represents the multi-dimensional coordinate of a physical chunk.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
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

    /// Local refinement level of every hash dimension.
    pub hash_levels: Vec<u16>,

    /// Local refinement level of every range dimension.
    pub range_levels: Vec<u16>,
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
        let hash_levels = vec![0; hash_buckets.len()];
        let range_levels = vec![0; range_buckets.len()];
        Self {
            row_bucket,
            level,
            col_group,
            hash_buckets,
            range_buckets,
            hash_levels,
            range_levels,
        }
    }

    /// Construct a physical chunk from an adaptive logical cell.
    pub fn from_cell(cell: &CellCoordinate, col_group: u16) -> Self {
        cell.with_col_group(col_group)
    }

    /// Return the coordinate without its physical column-group axis.
    pub fn cell(&self) -> CellCoordinate {
        CellCoordinate {
            row_bucket: self.row_bucket,
            row_level: self.level,
            hash_buckets: self.hash_buckets.clone(),
            range_buckets: self.range_buckets.clone(),
            hash_levels: self.hash_levels.clone(),
            range_levels: self.range_levels.clone(),
        }
    }

    pub fn dimensions_are_consistent(&self) -> bool {
        self.cell().dimensions_are_consistent()
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
            hash_levels: self.hash_levels.clone(),
            range_levels: self.range_levels.clone(),
        }
    }
}

/// Chunk with coordinate and version
#[derive(Debug, Clone)]
pub struct ChunkInfo {
    pub coord: ChunkCoordinate,
    pub version: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn physical_and_logical_coordinates_roundtrip() {
        let cell = CellCoordinate {
            row_bucket: 7,
            row_level: 2,
            hash_buckets: vec![3, 9],
            range_buckets: vec![42],
            hash_levels: vec![1, 0],
            range_levels: vec![4],
        };

        let physical = cell.with_col_group(5);
        assert_eq!(physical.cell(), cell);
        assert_eq!(physical.col_group, 5);
        assert!(physical.dimensions_are_consistent());
    }

    #[test]
    fn row_child_preserves_local_dimension_path() {
        let cell = CellCoordinate {
            row_bucket: 4,
            row_level: 1,
            hash_buckets: vec![11],
            range_buckets: vec![21],
            hash_levels: vec![3],
            range_levels: vec![2],
        };

        let child = cell.row_child(9);
        assert_eq!(child.row_bucket, 9);
        assert_eq!(child.row_level, 2);
        assert_eq!(child.hash_buckets, cell.hash_buckets);
        assert_eq!(child.range_buckets, cell.range_buckets);
        assert_eq!(child.hash_levels, cell.hash_levels);
        assert_eq!(child.range_levels, cell.range_levels);
    }
}

