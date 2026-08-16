use sled::Db;
use std::sync::Arc;
use xxhash_rust::xxh3::xxh3_64;
use crate::config::table_config::HashStrategy;
use crate::Result;

/// Maximum local hash refinement. The arithmetic also checks the configured
/// base bucket count, so a large base fanout can reach its limit earlier.
pub const MAX_HASH_LEVEL: u16 = 63;

/// Turn a stable raw hash into the bucket at one adaptive level.
///
/// Level `n` doubles the level-0 modulus `n` times. This is extendible-hash
/// routing: every refined child is a strict subset of its parent, and no
/// registry state is required.
pub fn hash_bucket_at_level(raw_hash: u64, base_buckets: u64, level: u16) -> Result<u64> {
    if base_buckets == 0 {
        return Err(crate::ChunkDbError::Config(
            "hash dimension num_buckets must be greater than zero".to_string(),
        ));
    }
    if level > MAX_HASH_LEVEL {
        return Err(crate::ChunkDbError::Config(format!(
            "hash refinement level {} exceeds maximum {}",
            level, MAX_HASH_LEVEL
        )));
    }

    let multiplier = 1u64.checked_shl(level as u32).ok_or_else(|| {
        crate::ChunkDbError::Config(format!("hash refinement level {} overflows", level))
    })?;
    let modulus = base_buckets.checked_mul(multiplier).ok_or_else(|| {
        crate::ChunkDbError::Config(format!(
            "hash bucket fanout {} * 2^{} overflows u64",
            base_buckets, level
        ))
    })?;
    Ok(raw_hash % modulus)
}

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
        self.bucket_for_raw_hash(Self::raw_hash_string(value), 0)
    }

    /// Get bucket for numeric value using xxh3 hash
    pub fn get_bucket_numeric(&self, value: i64) -> Result<u64> {
        self.bucket_for_raw_hash(Self::raw_hash_numeric(value), 0)
    }

    /// Lookup bucket for a known value
    /// For PureHash strategy, this always returns Some(bucket)
    pub fn lookup_bucket(&self, value: &str) -> Result<Option<u64>> {
        Ok(Some(self.get_bucket(value)?))
    }

    /// Lookup bucket for a numeric value
    /// For PureHash strategy, this always returns Some(bucket)
    pub fn lookup_bucket_numeric(&self, value: i64) -> Result<Option<u64>> {
        Ok(Some(self.get_bucket_numeric(value)?))
    }

    pub fn num_buckets(&self) -> u64 {
        self.num_buckets
    }

    pub fn raw_hash_string(value: &str) -> u64 {
        xxh3_64(value.as_bytes())
    }

    /// Numeric hash encoding used for all signed/unsigned integer widths.
    /// Narrow values are widened first, matching query `FilterValue::Int`.
    pub fn raw_hash_numeric(value: i64) -> u64 {
        xxh3_64(&value.to_le_bytes())
    }

    pub fn raw_hash_bool(value: bool) -> u64 {
        Self::raw_hash_string(if value { "true" } else { "false" })
    }

    pub fn bucket_for_raw_hash(&self, raw_hash: u64, level: u16) -> Result<u64> {
        hash_bucket_at_level(raw_hash, self.num_buckets, level)
    }

    pub fn lookup_bucket_at_level(&self, value: &str, level: u16) -> Result<u64> {
        self.bucket_for_raw_hash(Self::raw_hash_string(value), level)
    }

    pub fn lookup_bucket_numeric_at_level(&self, value: i64, level: u16) -> Result<u64> {
        self.bucket_for_raw_hash(Self::raw_hash_numeric(value), level)
    }

    pub fn lookup_bucket_bool_at_level(&self, value: bool, level: u16) -> Result<u64> {
        self.bucket_for_raw_hash(Self::raw_hash_bool(value), level)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refined_hash_bucket_is_nested() {
        let raw = HashRegistry::raw_hash_string("tenant-42");
        let base = 7;
        for level in 0..8 {
            let parent = hash_bucket_at_level(raw, base, level).unwrap();
            let child = hash_bucket_at_level(raw, base, level + 1).unwrap();
            let parent_modulus = base * (1u64 << level);
            assert!(child == parent || child == parent + parent_modulus);
        }
    }

    #[test]
    fn refined_hash_rejects_invalid_fanout() {
        assert!(hash_bucket_at_level(1, 0, 0).is_err());
        assert!(hash_bucket_at_level(1, u64::MAX, 1).is_err());
    }
}

