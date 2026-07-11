use std::collections::HashSet;
use std::sync::{RwLock, RwLockReadGuard};

use super::row_index::{bucket_at_level, MAX_SPLIT_LEVEL};

/// Per-table refinement map for the adaptive row grid.
///
/// Holds the set of row cells `(level, bucket)` that have been split. Routing
/// a row id descends by formula from level 0, recomputing the bucket one level
/// deeper while the current cell is marked refined — the map only says where
/// to stop. Invariant: every coordinate in the catalog sits on a leaf cell
/// (never on a refined one); split commits preserve this atomically.
///
/// Concurrency contract: writers that route rows and then record patches for
/// the resulting cells must do both under a single [`RoutingGuard`] (read
/// lock). `mark_refined` takes the write lock, so it acts as a barrier: once
/// it returns, every in-flight writer that routed to the split cell has
/// finished recording, and the split can safely drain the parent's patches.
#[derive(Debug)]
pub struct LevelMap {
    base_width: u64,
    refined: RwLock<HashSet<(u16, u64)>>,
}

/// Read guard over the refinement set: routes any number of row ids under a
/// single lock acquisition, and keeps concurrent `mark_refined` calls (splits)
/// blocked until dropped.
pub struct RoutingGuard<'a> {
    refined: RwLockReadGuard<'a, HashSet<(u16, u64)>>,
    base_width: u64,
}

impl RoutingGuard<'_> {
    /// Route a row id to its leaf cell.
    pub fn route(&self, row_id: u64) -> (u16, u64) {
        let mut level = 0u16;
        let mut bucket = bucket_at_level(row_id, self.base_width, 0);
        while self.refined.contains(&(level, bucket)) {
            // Defensive cap: with a validated snapshot the deepest refined
            // cell is MAX_SPLIT_LEVEL - 1, so the loop stops at or before
            // MAX_SPLIT_LEVEL by itself.
            if level >= MAX_SPLIT_LEVEL {
                break;
            }
            level += 1;
            bucket = bucket_at_level(row_id, self.base_width, level);
        }
        (level, bucket)
    }
}

impl LevelMap {
    pub fn new(base_width: u64) -> Self {
        Self {
            base_width,
            refined: RwLock::new(HashSet::new()),
        }
    }

    /// Rebuild from a persisted snapshot. Cells at or beyond MAX_SPLIT_LEVEL
    /// cannot have been produced by a legal split; reject the snapshot rather
    /// than route into undefined levels later.
    pub fn from_snapshot(base_width: u64, cells: Vec<(u16, u64)>) -> crate::Result<Self> {
        if let Some(&(level, bucket)) = cells.iter().find(|(l, _)| *l >= MAX_SPLIT_LEVEL) {
            return Err(crate::ChunkDbError::Serialization(format!(
                "corrupt level-map snapshot: refined cell ({}, {}) at or beyond MAX_SPLIT_LEVEL {}",
                level, bucket, MAX_SPLIT_LEVEL
            )));
        }
        Ok(Self {
            base_width,
            refined: RwLock::new(cells.into_iter().collect()),
        })
    }

    pub fn base_width(&self) -> u64 {
        self.base_width
    }

    /// Acquire the routing guard (read lock). Hold it across route + patch
    /// record so a concurrent split cannot drain patches in between.
    pub fn routing(&self) -> RoutingGuard<'_> {
        RoutingGuard {
            refined: self.refined.read().unwrap(),
            base_width: self.base_width,
        }
    }

    /// Route a single row id (convenience for one-off lookups; batch callers
    /// should use `routing()` once).
    pub fn route(&self, row_id: u64) -> (u16, u64) {
        self.routing().route(row_id)
    }

    /// Mark a cell as split (its children become the new leaves). Takes the
    /// write lock: returns only after every in-flight RoutingGuard is dropped.
    pub fn mark_refined(&self, level: u16, bucket: u64) {
        debug_assert!(level < MAX_SPLIT_LEVEL);
        self.refined.write().unwrap().insert((level, bucket));
    }

    pub fn is_refined(&self, level: u16, bucket: u64) -> bool {
        self.refined.read().unwrap().contains(&(level, bucket))
    }

    /// Snapshot for persistence (sorted for deterministic encoding).
    pub fn snapshot(&self) -> Vec<(u16, u64)> {
        let mut cells: Vec<_> = self.refined.read().unwrap().iter().copied().collect();
        cells.sort_unstable();
        cells
    }

    /// Snapshot with one extra cell included, without mutating the map — used
    /// by the split to persist the post-split state atomically *before*
    /// marking the cell refined in memory (so a failed commit leaves the
    /// in-memory map consistent with the catalog).
    pub fn snapshot_with(&self, cell: (u16, u64)) -> Vec<(u16, u64)> {
        let mut cells: Vec<_> = self.refined.read().unwrap().iter().copied().collect();
        cells.push(cell);
        cells.sort_unstable();
        cells.dedup();
        cells
    }

    pub fn is_empty(&self) -> bool {
        self.refined.read().unwrap().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_route_unrefined() {
        let map = LevelMap::new(100);
        assert_eq!(map.route(0), (0, 0));
        assert_eq!(map.route(99), (0, 0));
        assert_eq!(map.route(100), (0, 1));
    }

    #[test]
    fn test_route_descends_refinements() {
        let map = LevelMap::new(100);
        // Split cell (0,0) → children (1,0)=[0,50) and (1,1)=[50,100)
        map.mark_refined(0, 0);
        assert_eq!(map.route(0), (1, 0));
        assert_eq!(map.route(49), (1, 0));
        assert_eq!(map.route(50), (1, 1));
        assert_eq!(map.route(99), (1, 1));
        // Sibling cell untouched
        assert_eq!(map.route(100), (0, 1));

        // Split (1,1) → (2,2)=[50,75) and (2,3)=[75,100)
        map.mark_refined(1, 1);
        assert_eq!(map.route(50), (2, 2));
        assert_eq!(map.route(74), (2, 2));
        assert_eq!(map.route(75), (2, 3));
        assert_eq!(map.route(49), (1, 0));
    }

    #[test]
    fn test_routing_guard_batches_lookups() {
        let map = LevelMap::new(100);
        map.mark_refined(0, 0);
        let router = map.routing();
        assert_eq!(router.route(0), (1, 0));
        assert_eq!(router.route(99), (1, 1));
        assert_eq!(router.route(100), (0, 1));
    }

    #[test]
    fn test_snapshot_roundtrip() {
        let map = LevelMap::new(1000);
        map.mark_refined(0, 3);
        map.mark_refined(1, 7);
        let snap = map.snapshot();
        let restored = LevelMap::from_snapshot(1000, snap.clone()).unwrap();
        assert_eq!(restored.snapshot(), snap);
        assert!(restored.is_refined(0, 3));
        assert!(restored.is_refined(1, 7));
        assert!(!restored.is_refined(0, 0));
    }

    #[test]
    fn test_snapshot_with_does_not_mutate() {
        let map = LevelMap::new(1000);
        map.mark_refined(0, 3);
        let snap = map.snapshot_with((1, 6));
        assert_eq!(snap, vec![(0, 3), (1, 6)]);
        assert!(!map.is_refined(1, 6), "snapshot_with must not mutate the map");
        // Duplicates collapse
        assert_eq!(map.snapshot_with((0, 3)), vec![(0, 3)]);
    }

    #[test]
    fn test_from_snapshot_rejects_corrupt_levels() {
        let err = LevelMap::from_snapshot(1000, vec![(0, 1), (MAX_SPLIT_LEVEL, 0)]);
        assert!(err.is_err(), "cells at MAX_SPLIT_LEVEL cannot legally be refined");
    }
}
