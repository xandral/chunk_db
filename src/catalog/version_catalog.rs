use sled::Db;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use crate::storage::chunk_coord::ChunkCoordinate;
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

        Ok(Self { db, next_txn_id, next_row_id })
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
        match self.db.get(&key)? {
            Some(bytes) => {
                let arr: [u8; 8] = bytes.as_ref().try_into().unwrap_or([0u8; 8]);
                Ok(Some(u64::from_be_bytes(arr)))
            }
            None => Ok(None),
        }
    }

    /// Update version for a coordinate in a specific table
    pub fn update_version(&self, table_name: &str, coord: &ChunkCoordinate, version: u64) -> Result<()> {
        let key = self.coord_to_key(table_name, coord)?;
        self.db.insert(key, &version.to_be_bytes())?;
        Ok(())
    }

    /// List all chunk coordinates with their latest versions for a specific table.
    ///
    /// PERF: O(n) full scan of the sled catalog on every query. At scale (millions of chunks),
    /// this becomes a bottleneck. Consider caching or maintaining an in-memory index.
    pub fn all_chunks(&self, table_name: &str) -> Result<Vec<(ChunkCoordinate, u64)>> {
        let mut results = vec![];
        let prefix = format!("__chunk__{}__", table_name);

        for item in self.db.scan_prefix(prefix.as_bytes()) {
            let (key, value) = item?;

            // Extract coordinate from key (skip prefix)
            let coord_bytes = &key[prefix.len()..];
            if let Ok(coord) = bincode::deserialize::<ChunkCoordinate>(coord_bytes) {
                let arr: [u8; 8] = value.as_ref().try_into().unwrap_or([0u8; 8]);
                let version = u64::from_be_bytes(arr);
                results.push((coord, version));
            }
        }

        Ok(results)
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
