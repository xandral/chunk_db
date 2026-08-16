use std::collections::HashMap;
use std::sync::{RwLock, RwLockReadGuard};

use serde::{Deserialize, Serialize};

use crate::catalog::hash_bucket_at_level;
use crate::storage::CellCoordinate;
use crate::{ChunkDbError, Result};

use super::{range_bucket_at_level, range_level_is_splittable};

/// Non-row axis selected to refine one logical grid cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SplitAxis {
    Hash(usize),
    Range(usize),
}

/// Persisted representation of the local adaptive grid. A sorted vector keeps
/// bincode output deterministic and makes catalog snapshots easy to inspect.
pub type DimensionMapSnapshot = Vec<(CellCoordinate, SplitAxis)>;

/// Per-table map of logical cells that were locally refined along a hash or
/// range dimension.
///
/// A map entry belongs to an internal node; absence means the coordinate is a
/// physical leaf. Routing starts from level-zero dimension buckets inside the
/// row leaf selected by `LevelMap`, then recomputes one selected bucket at a
/// time until it reaches a leaf.
#[derive(Debug)]
pub struct DimensionMap {
    refined: RwLock<HashMap<CellCoordinate, SplitAxis>>,
}

pub struct DimensionRoutingGuard<'a> {
    refined: RwLockReadGuard<'a, HashMap<CellCoordinate, SplitAxis>>,
}

impl DimensionRoutingGuard<'_> {
    /// Route raw dimension values to a local leaf of the adaptive grid.
    pub fn route(
        &self,
        row_level: u16,
        row_bucket: u64,
        raw_hashes: &[u64],
        hash_base_buckets: &[u64],
        range_values: &[i64],
        range_chunk_sizes: &[u64],
    ) -> Result<CellCoordinate> {
        validate_route_arity(
            raw_hashes,
            hash_base_buckets,
            range_values,
            range_chunk_sizes,
        )?;

        let hash_buckets = raw_hashes
            .iter()
            .zip(hash_base_buckets)
            .map(|(&raw, &base)| hash_bucket_at_level(raw, base, 0))
            .collect::<Result<Vec<_>>>()?;
        let range_buckets = range_values
            .iter()
            .zip(range_chunk_sizes)
            .map(|(&value, &size)| range_bucket_at_level(value, size, 0))
            .collect::<Result<Vec<_>>>()?;

        let mut cell = CellCoordinate::base(
            row_bucket,
            row_level,
            hash_buckets,
            range_buckets,
        );

        // Every descent increments exactly one level, so a valid map cannot
        // cycle. The bound turns corrupt snapshots into a controlled error.
        let max_steps = raw_hashes
            .len()
            .saturating_add(range_values.len())
            .saturating_mul(64)
            .max(1);
        for _ in 0..max_steps {
            let Some(&axis) = self.refined.get(&cell) else {
                return Ok(cell);
            };
            cell = refine_cell(
                &cell,
                axis,
                raw_hashes,
                hash_base_buckets,
                range_values,
                range_chunk_sizes,
            )?;
        }

        Err(ChunkDbError::Serialization(
            "dimension-map route exceeded its maximum depth; snapshot is corrupt".to_string(),
        ))
    }
}

impl DimensionMap {
    pub fn new() -> Self {
        Self {
            refined: RwLock::new(HashMap::new()),
        }
    }

    pub fn from_snapshot(
        hash_base_buckets: &[u64],
        range_chunk_sizes: &[u64],
        snapshot: DimensionMapSnapshot,
    ) -> Result<Self> {
        let mut refined = HashMap::with_capacity(snapshot.len());
        for (cell, axis) in snapshot {
            validate_cell(&cell, hash_base_buckets, range_chunk_sizes)?;
            validate_axis(&cell, axis, hash_base_buckets, range_chunk_sizes)?;
            if let Some(previous) = refined.insert(cell.clone(), axis) {
                return Err(ChunkDbError::Serialization(format!(
                    "duplicate dimension-map cell {:?} ({:?} and {:?})",
                    cell, previous, axis
                )));
            }
        }
        Ok(Self {
            refined: RwLock::new(refined),
        })
    }

    pub fn routing(&self) -> DimensionRoutingGuard<'_> {
        DimensionRoutingGuard {
            refined: self.refined.read().unwrap(),
        }
    }

    pub fn route(
        &self,
        row_level: u16,
        row_bucket: u64,
        raw_hashes: &[u64],
        hash_base_buckets: &[u64],
        range_values: &[i64],
        range_chunk_sizes: &[u64],
    ) -> Result<CellCoordinate> {
        self.routing().route(
            row_level,
            row_bucket,
            raw_hashes,
            hash_base_buckets,
            range_values,
            range_chunk_sizes,
        )
    }

    /// Mark a logical leaf as refined. The write lock is also a routing
    /// barrier: after this returns no in-flight route can still stop there.
    pub fn mark_refined(&self, cell: CellCoordinate, axis: SplitAxis) {
        self.refined.write().unwrap().insert(cell, axis);
    }

    pub fn axis_for(&self, cell: &CellCoordinate) -> Option<SplitAxis> {
        self.refined.read().unwrap().get(cell).copied()
    }

    pub fn snapshot(&self) -> DimensionMapSnapshot {
        let mut entries: Vec<_> = self.refined.read().unwrap()
            .iter()
            .map(|(cell, &axis)| (cell.clone(), axis))
            .collect();
        entries.sort_unstable();
        entries
    }

    pub fn snapshot_with(
        &self,
        cell: CellCoordinate,
        axis: SplitAxis,
    ) -> DimensionMapSnapshot {
        let mut snapshot = self.snapshot();
        match snapshot.binary_search_by(|(existing, _)| existing.cmp(&cell)) {
            Ok(index) => snapshot[index].1 = axis,
            Err(index) => snapshot.insert(index, (cell, axis)),
        }
        snapshot
    }

    pub fn snapshot_without(&self, cell: &CellCoordinate) -> DimensionMapSnapshot {
        self.snapshot().into_iter()
            .filter(|(existing, _)| existing != cell)
            .collect()
    }

    /// Persisted snapshot for a global row split. The old row tree is replaced
    /// by identical copies rooted in both children; no dead routing history is
    /// accumulated in the catalog.
    pub fn snapshot_for_row_split(
        &self,
        row_level: u16,
        row_bucket: u64,
    ) -> DimensionMapSnapshot {
        let snapshot = self.snapshot();
        let mut expanded = Vec::with_capacity(snapshot.len().saturating_mul(2));
        for (cell, axis) in snapshot {
            if cell.row_level == row_level && cell.row_bucket == row_bucket {
                expanded.push((cell.row_child(row_bucket.saturating_mul(2)), axis));
                expanded.push((
                    cell.row_child(row_bucket.saturating_mul(2).saturating_add(1)),
                    axis,
                ));
            } else {
                expanded.push((cell, axis));
            }
        }
        expanded.sort_unstable();
        expanded.dedup();
        expanded
    }

    /// Transitional in-memory view used between catalog commit and the
    /// `LevelMap` routing barrier. It contains both the old tree and the two
    /// copies, so either row-map view has a complete local route.
    pub fn transition_snapshot_for_row_split(
        &self,
        row_level: u16,
        row_bucket: u64,
    ) -> DimensionMapSnapshot {
        let mut transition = self.snapshot();
        for (cell, axis) in self.snapshot() {
            if cell.row_level == row_level && cell.row_bucket == row_bucket {
                transition.push((cell.row_child(row_bucket.saturating_mul(2)), axis));
                transition.push((
                    cell.row_child(row_bucket.saturating_mul(2).saturating_add(1)),
                    axis,
                ));
            }
        }
        transition.sort_unstable();
        transition.dedup();
        transition
    }

    /// Replace the in-memory view after an atomic catalog commit.
    pub fn replace(&self, snapshot: DimensionMapSnapshot) {
        *self.refined.write().unwrap() = snapshot.into_iter().collect();
    }

    pub fn is_empty(&self) -> bool {
        self.refined.read().unwrap().is_empty()
    }
}

/// Test whether `child` is one of the two immediate descendants created by
/// refining `parent` along `axis`. Used by underfilled-sibling coalescing.
pub fn is_immediate_child(
    parent: &CellCoordinate,
    child: &CellCoordinate,
    axis: SplitAxis,
    hash_base_buckets: &[u64],
) -> Result<bool> {
    if parent.row_bucket != child.row_bucket
        || parent.row_level != child.row_level
        || parent.hash_buckets.len() != child.hash_buckets.len()
        || parent.range_buckets.len() != child.range_buckets.len()
        || !parent.dimensions_are_consistent()
        || !child.dimensions_are_consistent()
    {
        return Ok(false);
    }

    let mut expected = parent.clone();
    match axis {
        SplitAxis::Hash(index) => {
            if index >= parent.hash_buckets.len() || index >= hash_base_buckets.len() {
                return Ok(false);
            }
            let child_level = match parent.hash_levels[index].checked_add(1) {
                Some(level) => level,
                None => return Ok(false),
            };
            expected.hash_levels[index] = child_level;
            // Ignore the selected bucket for the structural comparison.
            expected.hash_buckets[index] = child.hash_buckets[index];
            if expected != *child {
                return Ok(false);
            }
            let parent_modulus = match hash_base_buckets[index]
                .checked_mul(1u64.checked_shl(parent.hash_levels[index] as u32).unwrap_or(0))
            {
                Some(value) if value > 0 => value,
                _ => return Ok(false),
            };
            Ok(child.hash_buckets[index] == parent.hash_buckets[index]
                || child.hash_buckets[index]
                    == parent.hash_buckets[index].saturating_add(parent_modulus))
        }
        SplitAxis::Range(index) => {
            if index >= parent.range_buckets.len() {
                return Ok(false);
            }
            let child_level = match parent.range_levels[index].checked_add(1) {
                Some(level) => level,
                None => return Ok(false),
            };
            expected.range_levels[index] = child_level;
            expected.range_buckets[index] = child.range_buckets[index];
            if expected != *child {
                return Ok(false);
            }
            let parent_signed = ordered_u64_to_i64(parent.range_buckets[index]) as i128;
            let child_signed = ordered_u64_to_i64(child.range_buckets[index]) as i128;
            Ok(child_signed.div_euclid(2) == parent_signed)
        }
    }
}

fn ordered_u64_to_i64(value: u64) -> i64 {
    (value ^ (1u64 << 63)) as i64
}

impl Default for DimensionMap {
    fn default() -> Self {
        Self::new()
    }
}

/// Descend one local split for a known row's raw dimension values.
pub fn refine_cell(
    parent: &CellCoordinate,
    axis: SplitAxis,
    raw_hashes: &[u64],
    hash_base_buckets: &[u64],
    range_values: &[i64],
    range_chunk_sizes: &[u64],
) -> Result<CellCoordinate> {
    validate_route_arity(
        raw_hashes,
        hash_base_buckets,
        range_values,
        range_chunk_sizes,
    )?;
    if !parent.dimensions_are_consistent()
        || parent.hash_buckets.len() != raw_hashes.len()
        || parent.range_buckets.len() != range_values.len()
    {
        return Err(ChunkDbError::Serialization(format!(
            "dimension-map cell arity does not match table configuration: {:?}",
            parent
        )));
    }

    let mut child = parent.clone();
    match axis {
        SplitAxis::Hash(index) => {
            let level = child.hash_levels.get(index).copied().ok_or_else(|| {
                ChunkDbError::Serialization(format!(
                    "hash split dimension {} is outside coordinate {:?}", index, parent
                ))
            })?;
            let child_level = level.checked_add(1).ok_or_else(|| {
                ChunkDbError::Config("hash refinement level overflow".to_string())
            })?;
            child.hash_levels[index] = child_level;
            child.hash_buckets[index] = hash_bucket_at_level(
                raw_hashes[index],
                hash_base_buckets[index],
                child_level,
            )?;
        }
        SplitAxis::Range(index) => {
            let level = child.range_levels.get(index).copied().ok_or_else(|| {
                ChunkDbError::Serialization(format!(
                    "range split dimension {} is outside coordinate {:?}", index, parent
                ))
            })?;
            if !range_level_is_splittable(range_chunk_sizes[index], level) {
                return Err(ChunkDbError::Config(format!(
                    "range dimension {} with chunk size {} cannot split level {}",
                    index, range_chunk_sizes[index], level
                )));
            }
            let child_level = level + 1;
            child.range_levels[index] = child_level;
            child.range_buckets[index] = range_bucket_at_level(
                range_values[index],
                range_chunk_sizes[index],
                child_level,
            )?;
        }
    }
    Ok(child)
}

fn validate_route_arity(
    raw_hashes: &[u64],
    hash_base_buckets: &[u64],
    range_values: &[i64],
    range_chunk_sizes: &[u64],
) -> Result<()> {
    if raw_hashes.len() != hash_base_buckets.len()
        || range_values.len() != range_chunk_sizes.len()
    {
        return Err(ChunkDbError::Config(format!(
            "dimension value/config arity mismatch: {} hash values for {} axes, {} range values for {} axes",
            raw_hashes.len(),
            hash_base_buckets.len(),
            range_values.len(),
            range_chunk_sizes.len()
        )));
    }
    Ok(())
}

fn validate_cell(
    cell: &CellCoordinate,
    hash_base_buckets: &[u64],
    range_chunk_sizes: &[u64],
) -> Result<()> {
    if !cell.dimensions_are_consistent()
        || cell.hash_buckets.len() != hash_base_buckets.len()
        || cell.range_buckets.len() != range_chunk_sizes.len()
    {
        return Err(ChunkDbError::Serialization(format!(
            "dimension-map coordinate arity does not match table: {:?}", cell
        )));
    }
    Ok(())
}

fn validate_axis(
    cell: &CellCoordinate,
    axis: SplitAxis,
    hash_base_buckets: &[u64],
    range_chunk_sizes: &[u64],
) -> Result<()> {
    match axis {
        SplitAxis::Hash(index) => {
            let level = *cell.hash_levels.get(index).ok_or_else(|| {
                ChunkDbError::Serialization(format!(
                    "dimension map references missing hash axis {}", index
                ))
            })?;
            // Validate that the next-level modulus is representable.
            hash_bucket_at_level(0, hash_base_buckets[index], level.saturating_add(1))?;
        }
        SplitAxis::Range(index) => {
            let level = *cell.range_levels.get(index).ok_or_else(|| {
                ChunkDbError::Serialization(format!(
                    "dimension map references missing range axis {}", index
                ))
            })?;
            if !range_level_is_splittable(range_chunk_sizes[index], level) {
                return Err(ChunkDbError::Serialization(format!(
                    "dimension map refines unsplittable range axis {} at level {}",
                    index, level
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::HashRegistry;

    #[test]
    fn routing_descends_different_local_axes() {
        let raw_hash = HashRegistry::raw_hash_string("tenant-7");
        let base_hash = [4];
        let ranges = [37];
        let range_sizes = [100];

        let map = DimensionMap::new();
        let root = map.route(0, 2, &[raw_hash], &base_hash, &ranges, &range_sizes).unwrap();
        map.mark_refined(root.clone(), SplitAxis::Hash(0));
        let hash_child = map.route(0, 2, &[raw_hash], &base_hash, &ranges, &range_sizes).unwrap();
        assert_eq!(hash_child.hash_levels, vec![1]);

        map.mark_refined(hash_child.clone(), SplitAxis::Range(0));
        let leaf = map.route(0, 2, &[raw_hash], &base_hash, &ranges, &range_sizes).unwrap();
        assert_eq!(leaf.hash_levels, vec![1]);
        assert_eq!(leaf.range_levels, vec![1]);
        assert_ne!(leaf, hash_child);
    }

    #[test]
    fn snapshot_roundtrip_is_deterministic() {
        let map = DimensionMap::new();
        let a = CellCoordinate::base(2, 0, vec![1], vec![3]);
        let b = CellCoordinate::base(1, 0, vec![0], vec![4]);
        map.mark_refined(a, SplitAxis::Hash(0));
        map.mark_refined(b, SplitAxis::Range(0));

        let snapshot = map.snapshot();
        let restored = DimensionMap::from_snapshot(&[4], &[100], snapshot.clone()).unwrap();
        assert_eq!(restored.snapshot(), snapshot);
    }

    #[test]
    fn row_split_copies_dimension_tree_to_both_children() {
        let map = DimensionMap::new();
        let root = CellCoordinate::base(3, 0, vec![1], vec![]);
        map.mark_refined(root.clone(), SplitAxis::Hash(0));

        let snapshot = map.snapshot_for_row_split(0, 3);
        assert!(!snapshot.contains(&(root.clone(), SplitAxis::Hash(0))));
        assert!(snapshot.contains(&(root.row_child(6), SplitAxis::Hash(0))));
        assert!(snapshot.contains(&(root.row_child(7), SplitAxis::Hash(0))));

        let transition = map.transition_snapshot_for_row_split(0, 3);
        assert!(transition.contains(&(root, SplitAxis::Hash(0))));
    }

    #[test]
    fn recognizes_hash_and_negative_range_children() {
        let hash_parent = CellCoordinate::base(0, 0, vec![3], vec![]);
        let mut hash_child = hash_parent.clone();
        hash_child.hash_levels[0] = 1;
        hash_child.hash_buckets[0] = 7; // parent 3 + level-0 modulus 4
        assert!(is_immediate_child(
            &hash_parent,
            &hash_child,
            SplitAxis::Hash(0),
            &[4],
        ).unwrap());

        let range_parent = CellCoordinate::base(
            0,
            0,
            vec![],
            vec![range_bucket_at_level(-75, 100, 0).unwrap()],
        );
        let mut range_child = range_parent.clone();
        range_child.range_levels[0] = 1;
        range_child.range_buckets[0] = range_bucket_at_level(-75, 100, 1).unwrap();
        assert!(is_immediate_child(
            &range_parent,
            &range_child,
            SplitAxis::Range(0),
            &[],
        ).unwrap());
    }
}
