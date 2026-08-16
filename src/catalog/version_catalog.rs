use sled::Db;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use serde::{Deserialize, Serialize};

use crate::partitioning::DimensionMapSnapshot;
use crate::storage::chunk_coord::{CellCoordinate, ChunkCoordinate};
use crate::catalog::range_stats::RangeDimensionStats;
use crate::config::table_config::TableConfig;
use crate::Result;

/// Tracks the latest version for each chunk coordinate
#[derive(Debug)]
pub struct VersionCatalog {
    db: Db,
    /// Global transaction counter
    next_txn_id: AtomicU64,
    /// Global row ID counter (Snowflake-like)
    next_row_id: AtomicU64,
    /// In-memory coordinate index per table, loaded lazily from sled and kept
    /// in sync by every version update. Serves all_chunks() without the
    /// per-query O(n) sled scan.
    chunk_index: RwLock<HashMap<String, HashMap<ChunkCoordinate, u64>>>,
}

/// Coordinate encoding written before local hash/range levels existed. It is
/// retained solely for transparent catalog-key migration on first rewrite.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyChunkCoordinate {
    row_bucket: u64,
    level: u16,
    col_group: u16,
    hash_buckets: Vec<u64>,
    range_buckets: Vec<u64>,
}

impl From<LegacyChunkCoordinate> for ChunkCoordinate {
    fn from(legacy: LegacyChunkCoordinate) -> Self {
        ChunkCoordinate {
            row_bucket: legacy.row_bucket,
            level: legacy.level,
            col_group: legacy.col_group,
            hash_levels: vec![0; legacy.hash_buckets.len()],
            range_levels: vec![0; legacy.range_buckets.len()],
            hash_buckets: legacy.hash_buckets,
            range_buckets: legacy.range_buckets,
        }
    }
}

impl VersionCatalog {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = sled::open(path)?;

        // Load or initialize transaction counter
        let next_txn_id = match db.get("__next_txn_id__")? {
            Some(bytes) => {
                let arr: [u8; 8] = bytes.as_ref().try_into().unwrap_or([0u8; 8]);
                AtomicU64::new(u64::from_be_bytes(arr))
            }
            None => AtomicU64::new(1),
        };

        // Load or initialize row ID counter
        let next_row_id = match db.get("__next_row_id__")? {
            Some(bytes) => {
                let arr: [u8; 8] = bytes.as_ref().try_into().unwrap_or([0u8; 8]);
                AtomicU64::new(u64::from_be_bytes(arr))
            }
            None => AtomicU64::new(0),  // Start from 0
        };

        Ok(Self {
            db,
            next_txn_id,
            next_row_id,
            chunk_index: RwLock::new(HashMap::new()),
        })
    }

    /// Generate next transaction ID (monotonically increasing)
    pub fn next_transaction_id(&self) -> Result<u64> {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);

        // Persist counter
        self.db.insert("__next_txn_id__", &(txn_id + 1).to_be_bytes())?;
        self.db.flush()?;

        Ok(txn_id)
    }

    /// Get current transaction ID (for reads)
    pub fn current_transaction_id(&self) -> u64 {
        self.next_txn_id.load(Ordering::SeqCst).saturating_sub(1)
    }

    /// Get latest version for a coordinate in a specific table
    pub fn get_latest_version(&self, table_name: &str, coord: &ChunkCoordinate) -> Result<Option<u64>> {
        let key = self.coord_to_key(table_name, coord)?;
        if let Some(bytes) = self.db.get(&key)? {
            return Ok(Some(decode_version(&bytes)));
        }

        // A pre-adaptive coordinate has a different bincode key. Fall back
        // only for a fully level-zero coordinate; refined cells never had a
        // legacy representation.
        if let Some(legacy_key) = self.legacy_coord_to_key(table_name, coord)? {
            if let Some(bytes) = self.db.get(legacy_key)? {
                return Ok(Some(decode_version(&bytes)));
            }
        }
        Ok(None)
    }

    /// Update version for a coordinate in a specific table
    pub fn update_version(&self, table_name: &str, coord: &ChunkCoordinate, version: u64) -> Result<()> {
        let key = self.coord_to_key(table_name, coord)?;
        let mut batch = sled::Batch::default();
        batch.insert(key, &version.to_be_bytes());
        if let Some(legacy_key) = self.legacy_coord_to_key(table_name, coord)? {
            batch.remove(legacy_key);
        }
        self.db.apply_batch(batch)?;

        let mut index = self.chunk_index.write().unwrap();
        if let Some(table_index) = index.get_mut(table_name) {
            table_index.insert(coord.clone(), version);
        }
        Ok(())
    }

    /// Load a table's coordinates from sled into the in-memory index (once).
    ///
    /// The write lock is held across the whole sled scan: update_version /
    /// commit_split write sled first and take this same lock second, so a
    /// version update landing mid-scan blocks here and is then re-applied on
    /// the freshly loaded entry — no update can fall between the scan and the
    /// conditional in-memory maintenance.
    fn ensure_index_loaded(&self, table_name: &str) -> Result<()> {
        {
            let index = self.chunk_index.read().unwrap();
            if index.contains_key(table_name) {
                return Ok(());
            }
        }

        let mut index = self.chunk_index.write().unwrap();
        if index.contains_key(table_name) {
            return Ok(()); // another thread loaded it while we waited
        }

        let mut table_index: HashMap<ChunkCoordinate, u64> = HashMap::new();
        let prefix = format!("__chunk__{}__", table_name);

        for item in self.db.scan_prefix(prefix.as_bytes()) {
            let (key, value) = item?;
            let coord_bytes = &key[prefix.len()..];
            let coord = decode_coordinate(coord_bytes).map_err(|e| {
                crate::ChunkDbError::Serialization(format!(
                    "undecodable chunk coordinate key for table '{}': {}",
                    table_name, e
                ))
            })?;
            let arr: [u8; 8] = value.as_ref().try_into().unwrap_or([0u8; 8]);
            let version = u64::from_be_bytes(arr);
            table_index
                .entry(coord)
                .and_modify(|existing| *existing = (*existing).max(version))
                .or_insert(version);
        }

        index.insert(table_name.to_string(), table_index);
        Ok(())
    }

    /// List all chunk coordinates with their latest versions for a specific table.
    /// Served from the in-memory index (sled is scanned once, at first access).
    pub fn all_chunks(&self, table_name: &str) -> Result<Vec<(ChunkCoordinate, u64)>> {
        self.ensure_index_loaded(table_name)?;
        let index = self.chunk_index.read().unwrap();
        Ok(index.get(table_name)
            .map(|m| m.iter().map(|(c, &v)| (c.clone(), v)).collect())
            .unwrap_or_default())
    }

    /// Coordinates (with versions) of one row cell — every hash/range/column
    /// group combination sharing `(level, row_bucket)`. Same freshness as
    /// all_chunks() without cloning the whole table index.
    pub fn chunks_for_cell(
        &self,
        table_name: &str,
        level: u16,
        row_bucket: u64,
    ) -> Result<Vec<(ChunkCoordinate, u64)>> {
        self.ensure_index_loaded(table_name)?;
        let index = self.chunk_index.read().unwrap();
        Ok(index.get(table_name)
            .map(|m| m.iter()
                .filter(|(c, _)| c.level == level && c.row_bucket == row_bucket)
                .map(|(c, &v)| (c.clone(), v))
                .collect())
            .unwrap_or_default())
    }

    /// Physical column-group chunks for exactly one logical adaptive leaf.
    pub fn chunks_for_logical_cell(
        &self,
        table_name: &str,
        cell: &CellCoordinate,
    ) -> Result<Vec<(ChunkCoordinate, u64)>> {
        self.ensure_index_loaded(table_name)?;
        let index = self.chunk_index.read().unwrap();
        Ok(index.get(table_name)
            .map(|m| m.iter()
                .filter(|(coord, _)| coord.cell() == *cell)
                .map(|(coord, &version)| (coord.clone(), version))
                .collect())
            .unwrap_or_default())
    }

    /// Atomically commit a row-cell split: children become visible, parent
    /// coordinates disappear, and the level-map snapshot is persisted — all in
    /// one sled batch. This is the crash-consistency point of the split: a
    /// crash before leaves the parent intact (orphan child files are
    /// harmless), a crash after is fully consistent.
    pub fn commit_split(
        &self,
        table_name: &str,
        parents: &[ChunkCoordinate],
        children: &[(ChunkCoordinate, u64)],
        level_map_snapshot: &[(u16, u64)],
    ) -> Result<()> {
        self.commit_grid_change(
            table_name,
            parents,
            children,
            Some(level_map_snapshot),
            None,
        )
    }

    /// Atomically commit a global row split together with both adaptive-grid
    /// snapshots. Child files are written before this point and are harmless
    /// orphans if the batch does not commit.
    pub fn commit_row_split(
        &self,
        table_name: &str,
        parents: &[ChunkCoordinate],
        children: &[(ChunkCoordinate, u64)],
        level_map_snapshot: &[(u16, u64)],
        dimension_map_snapshot: &DimensionMapSnapshot,
    ) -> Result<()> {
        self.commit_grid_change(
            table_name,
            parents,
            children,
            Some(level_map_snapshot),
            Some(dimension_map_snapshot),
        )
    }

    /// Atomically replace one logical leaf with locally refined hash/range
    /// children and persist the corresponding dimension-map internal node.
    pub fn commit_dimension_split(
        &self,
        table_name: &str,
        parents: &[ChunkCoordinate],
        children: &[(ChunkCoordinate, u64)],
        dimension_map_snapshot: &DimensionMapSnapshot,
    ) -> Result<()> {
        self.commit_grid_change(
            table_name,
            parents,
            children,
            None,
            Some(dimension_map_snapshot),
        )
    }

    /// Same atomic leaf replacement used in the reverse direction when
    /// underfilled local siblings are coalesced into their parent.
    pub fn commit_dimension_merge(
        &self,
        table_name: &str,
        children: &[ChunkCoordinate],
        parents: &[(ChunkCoordinate, u64)],
        dimension_map_snapshot: &DimensionMapSnapshot,
    ) -> Result<()> {
        self.commit_grid_change(
            table_name,
            children,
            parents,
            None,
            Some(dimension_map_snapshot),
        )
    }

    fn commit_grid_change(
        &self,
        table_name: &str,
        parents: &[ChunkCoordinate],
        children: &[(ChunkCoordinate, u64)],
        level_map_snapshot: Option<&[(u16, u64)]>,
        dimension_map_snapshot: Option<&DimensionMapSnapshot>,
    ) -> Result<()> {
        let mut batch = sled::Batch::default();

        for parent in parents {
            batch.remove(self.coord_to_key(table_name, parent)?);
            if let Some(legacy_key) = self.legacy_coord_to_key(table_name, parent)? {
                batch.remove(legacy_key);
            }
        }
        for (child, version) in children {
            batch.insert(self.coord_to_key(table_name, child)?, &version.to_be_bytes());
            if let Some(legacy_key) = self.legacy_coord_to_key(table_name, child)? {
                batch.remove(legacy_key);
            }
        }

        if let Some(snapshot) = level_map_snapshot {
            let lm_key = format!("__level_map__{}", table_name);
            let lm_bytes = bincode::serialize(snapshot)
                .map_err(|e| crate::ChunkDbError::Serialization(e.to_string()))?;
            batch.insert(lm_key.as_bytes(), lm_bytes);
        }
        if let Some(snapshot) = dimension_map_snapshot {
            let dm_key = format!("__dimension_map__{}", table_name);
            let dm_bytes = bincode::serialize(snapshot)
                .map_err(|e| crate::ChunkDbError::Serialization(e.to_string()))?;
            batch.insert(dm_key.as_bytes(), dm_bytes);
        }

        self.db.apply_batch(batch)?;
        self.db.flush()?;

        let mut index = self.chunk_index.write().unwrap();
        if let Some(table_index) = index.get_mut(table_name) {
            for parent in parents {
                table_index.remove(parent);
            }
            for (child, version) in children {
                table_index.insert(child.clone(), *version);
            }
        }
        Ok(())
    }

    /// Load the persisted level-map snapshot for a table (empty if absent).
    pub fn load_level_map(&self, table_name: &str) -> Result<Vec<(u16, u64)>> {
        let key = format!("__level_map__{}", table_name);
        match self.db.get(key.as_bytes())? {
            Some(bytes) => bincode::deserialize(&bytes)
                .map_err(|e| crate::ChunkDbError::Serialization(e.to_string())),
            None => Ok(vec![]),
        }
    }

    /// Load the local hash/range refinement map (empty for legacy tables).
    pub fn load_dimension_map(&self, table_name: &str) -> Result<DimensionMapSnapshot> {
        let key = format!("__dimension_map__{}", table_name);
        match self.db.get(key.as_bytes())? {
            Some(bytes) => bincode::deserialize(&bytes)
                .map_err(|e| crate::ChunkDbError::Serialization(e.to_string())),
            None => Ok(vec![]),
        }
    }

    fn coord_to_key(&self, table_name: &str, coord: &ChunkCoordinate) -> Result<Vec<u8>> {
        // Create key with table name prefix: __chunk__{table}__[coord_bytes]
        let coord_bytes = bincode::serialize(coord)
            .map_err(|e| crate::ChunkDbError::Serialization(e.to_string()))?;

        let prefix = format!("__chunk__{}__", table_name);
        let mut key = prefix.into_bytes();
        key.extend_from_slice(&coord_bytes);

        Ok(key)
    }

    fn legacy_coord_to_key(
        &self,
        table_name: &str,
        coord: &ChunkCoordinate,
    ) -> Result<Option<Vec<u8>>> {
        if coord.hash_levels.iter().any(|&level| level != 0)
            || coord.range_levels.iter().any(|&level| level != 0)
        {
            return Ok(None);
        }
        let legacy = LegacyChunkCoordinate {
            row_bucket: coord.row_bucket,
            level: coord.level,
            col_group: coord.col_group,
            hash_buckets: coord.hash_buckets.clone(),
            range_buckets: coord.range_buckets.clone(),
        };
        let coord_bytes = bincode::serialize(&legacy)
            .map_err(|e| crate::ChunkDbError::Serialization(e.to_string()))?;
        let prefix = format!("__chunk__{}__", table_name);
        let mut key = prefix.into_bytes();
        key.extend_from_slice(&coord_bytes);
        Ok(Some(key))
    }

    /// Get range statistics for a table/column
    pub fn get_range_stats(&self, table: &str, column: &str) -> Result<Option<RangeDimensionStats>> {
        let key = format!("__range_stats__{}_{}", table, column);
        match self.db.get(key.as_bytes())? {
            Some(bytes) => {
                let stats: RangeDimensionStats = bincode::deserialize(&bytes)
                    .map_err(|e| crate::ChunkDbError::Serialization(e.to_string()))?;
                Ok(Some(stats))
            }
            None => Ok(None),
        }
    }

    /// Update range statistics for a table/column
    pub fn update_range_stats(&self, table: &str, column: &str, stats: &RangeDimensionStats) -> Result<()> {
        let key = format!("__range_stats__{}_{}", table, column);
        let bytes = bincode::serialize(stats)
            .map_err(|e| crate::ChunkDbError::Serialization(e.to_string()))?;
        self.db.insert(key.as_bytes(), bytes)?;
        Ok(())
    }

    /// Get all range stats for a table
    pub fn get_all_range_stats(&self, table: &str) -> Result<Vec<RangeDimensionStats>> {
        let prefix = format!("__range_stats__{}_", table);
        let mut results = vec![];

        for item in self.db.scan_prefix(prefix.as_bytes()) {
            let (_, value) = item?;
            if let Ok(stats) = bincode::deserialize::<RangeDimensionStats>(&value) {
                results.push(stats);
            }
        }

        Ok(results)
    }

    /// List all tables
    pub fn list_tables(&self) -> Result<Vec<String>> {
        let mut tables = vec![];
        let prefix = "__table_config__";

        for item in self.db.scan_prefix(prefix.as_bytes()) {
            let (key, _) = item?;
            if let Ok(key_str) = std::str::from_utf8(&key) {
                if let Some(table_name) = key_str.strip_prefix(prefix) {
                    tables.push(table_name.to_string());
                }
            }
        }

        Ok(tables)
    }

    /// Save table configuration
    pub fn save_table_config(&self, config: &TableConfig) -> Result<()> {
        let key = format!("__table_config__{}", config.name);
        let bytes = bincode::serialize(config)
            .map_err(|e| crate::ChunkDbError::Serialization(e.to_string()))?;
        self.db.insert(key.as_bytes(), bytes)?;
        self.db.flush()?;
        Ok(())
    }

    /// Load table configuration
    pub fn load_table_config(&self, table_name: &str) -> Result<Option<TableConfig>> {
        let key = format!("__table_config__{}", table_name);
        match self.db.get(key.as_bytes())? {
            Some(bytes) => {
                let config: TableConfig = bincode::deserialize(&bytes)
                    .map_err(|e| crate::ChunkDbError::Serialization(e.to_string()))?;
                Ok(Some(config))
            }
            None => Ok(None),
        }
    }

    pub fn flush(&self) -> Result<()> {
        self.db.flush()?;
        Ok(())
    }


    /// Allocate a batch of sequential row IDs (Snowflake strategy).
    ///
    /// Persists the counter to disk after every allocation, ensuring
    /// crash safety: no IDs can be reused after recovery.
    pub fn allocate_row_ids(&self, count: u64) -> Result<u64> {
        let start_id = self.next_row_id.fetch_add(count, Ordering::SeqCst);
        self.db.insert("__next_row_id__", &(start_id + count).to_be_bytes())?;
        Ok(start_id)
    }
}

fn decode_coordinate(bytes: &[u8]) -> std::result::Result<ChunkCoordinate, bincode::Error> {
    match bincode::deserialize::<ChunkCoordinate>(bytes) {
        Ok(coord) => Ok(coord),
        Err(new_error) => match bincode::deserialize::<LegacyChunkCoordinate>(bytes) {
            Ok(legacy) => Ok(legacy.into()),
            Err(_) => Err(new_error),
        },
    }
}

fn decode_version(bytes: &[u8]) -> u64 {
    let arr: [u8; 8] = bytes.try_into().unwrap_or([0u8; 8]);
    u64::from_be_bytes(arr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_coordinate_keys_are_read_and_migrated_on_write() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = VersionCatalog::open(dir.path()).unwrap();
        let coord = ChunkCoordinate::new(3, 1, vec![7], vec![9]);
        let legacy_key = catalog.legacy_coord_to_key("events", &coord)
            .unwrap()
            .unwrap();
        catalog.db.insert(&legacy_key, &4u64.to_be_bytes()).unwrap();

        assert_eq!(catalog.get_latest_version("events", &coord).unwrap(), Some(4));
        let chunks = catalog.all_chunks("events").unwrap();
        assert_eq!(chunks, vec![(coord.clone(), 4)]);

        catalog.update_version("events", &coord, 5).unwrap();
        assert!(catalog.db.get(legacy_key).unwrap().is_none());
        assert_eq!(catalog.get_latest_version("events", &coord).unwrap(), Some(5));
    }

    #[test]
    fn new_coordinate_encoding_preserves_local_levels() {
        let coord = ChunkCoordinate {
            row_bucket: 2,
            level: 1,
            col_group: 0,
            hash_buckets: vec![5],
            range_buckets: vec![8],
            hash_levels: vec![3],
            range_levels: vec![2],
        };
        let bytes = bincode::serialize(&coord).unwrap();
        assert_eq!(decode_coordinate(&bytes).unwrap(), coord);
    }
}
