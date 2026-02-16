use sled::Db;
use std::sync::Arc;
use xxhash_rust::xxh3::xxh3_64;
use crate::config::table_config::HashStrategy;
use crate::Result;

/// Registry for hash partitioning
///
/// Currently implements only PureHash strategy (stateless xxh3 hashing).
/// Future strategies (e.g., Counter for sequential assignment) could be added here.
#[derive(Debug, Clone)]
pub struct HashRegistry {
    num_buckets: u64,
}

impl HashRegistry {
    pub fn new(_db: Arc<Db>, _column_name: &str, num_buckets: u64, _strategy: HashStrategy) -> Self {
        // Note: db, column_name, and strategy parameters are kept for API compatibility
        // but not used by PureHash strategy. Future strategies may require them.
        Self {
            num_buckets,
        }
    }

    /// Get bucket for a string value using xxh3 hash
    pub fn get_bucket(&self, value: &str) -> Result<u64> {
        Ok(xxh3_64(value.as_bytes()) % self.num_buckets)
    }

    /// Get bucket for numeric value using xxh3 hash
    pub fn get_bucket_numeric(&self, value: i64) -> Result<u64> {
        Ok(xxh3_64(&value.to_le_bytes()) % self.num_buckets)
    }

    /// Lookup bucket for a known value
    /// For PureHash strategy, this always returns Some(bucket)
    pub fn lookup_bucket(&self, value: &str) -> Result<Option<u64>> {
        Ok(Some(xxh3_64(value.as_bytes()) % self.num_buckets))
    }

    /// Lookup bucket for a numeric value
    /// For PureHash strategy, this always returns Some(bucket)
    pub fn lookup_bucket_numeric(&self, value: i64) -> Result<Option<u64>> {
        Ok(Some(xxh3_64(&value.to_le_bytes()) % self.num_buckets))
    }
}


