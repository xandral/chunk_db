use std::collections::HashMap;
use std::sync::RwLock;
use arrow::record_batch::RecordBatch;

#[derive(Debug, Clone)]
struct CacheEntry {
    batch: RecordBatch,
    last_applied_tx: u64,
}

pub struct ChunkCache {
    entries: RwLock<HashMap<Vec<u8>, CacheEntry>>,
    max_entries: usize,
}

impl ChunkCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            max_entries,
        }
    }

    pub fn get(&self, chunk_key: &[u8], current_max_tx: u64) -> Option<RecordBatch> {
        let map = self.entries.read().unwrap();
        map.get(chunk_key)
            .filter(|e| e.last_applied_tx == current_max_tx)
            .map(|e| e.batch.clone())
    }

    pub fn get_with_tx(&self, chunk_key: &[u8]) -> Option<(RecordBatch, u64)> {
        let map = self.entries.read().unwrap();
        map.get(chunk_key).map(|e| (e.batch.clone(), e.last_applied_tx))
    }

    pub fn put(&self, chunk_key: Vec<u8>, batch: RecordBatch, applied_tx: u64) {
        let mut map = self.entries.write().unwrap();
        if map.len() >= self.max_entries && !map.contains_key(&chunk_key) {
            // Evict one entry (first key found)
            if let Some(key) = map.keys().next().cloned() {
                map.remove(&key);
            }
        }
        map.insert(chunk_key, CacheEntry { batch, last_applied_tx: applied_tx });
    }

    pub fn invalidate(&self, chunk_key: &[u8]) {
        let mut map = self.entries.write().unwrap();
        map.remove(chunk_key);
    }

    pub fn clear(&self) {
        let mut map = self.entries.write().unwrap();
        map.clear();
    }

    pub fn len(&self) -> usize {
        let map = self.entries.read().unwrap();
        map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(row_ids: Vec<u64>, values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("__row_id", DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(UInt64Array::from(row_ids)),
            Arc::new(Int64Array::from(values)),
        ]).unwrap()
    }

    #[test]
    fn test_cache_miss() {
        let cache = ChunkCache::new(100);
        assert!(cache.get(b"c1", 0).is_none());
    }

    #[test]
    fn test_cache_hit() {
        let cache = ChunkCache::new(100);
        let batch = make_batch(vec![1], vec![10]);
        cache.put(b"c1".to_vec(), batch, 5);
        assert!(cache.get(b"c1", 5).is_some());
    }

    #[test]
    fn test_cache_stale() {
        let cache = ChunkCache::new(100);
        let batch = make_batch(vec![1], vec![10]);
        cache.put(b"c1".to_vec(), batch, 5);
        assert!(cache.get(b"c1", 8).is_none());
    }

    #[test]
    fn test_get_with_tx_returns_stale() {
        let cache = ChunkCache::new(100);
        let batch = make_batch(vec![1], vec![10]);
        cache.put(b"c1".to_vec(), batch, 5);
        let result = cache.get_with_tx(b"c1");
        assert!(result.is_some());
        let (_, tx) = result.unwrap();
        assert_eq!(tx, 5);
    }

    #[test]
    fn test_invalidate() {
        let cache = ChunkCache::new(100);
        cache.put(b"c1".to_vec(), make_batch(vec![1], vec![10]), 5);
        cache.invalidate(b"c1");
        assert!(cache.get(b"c1", 5).is_none());
    }

    #[test]
    fn test_eviction() {
        let cache = ChunkCache::new(2);
        cache.put(b"c1".to_vec(), make_batch(vec![1], vec![10]), 1);
        cache.put(b"c2".to_vec(), make_batch(vec![2], vec![20]), 2);
        cache.put(b"c3".to_vec(), make_batch(vec![3], vec![30]), 3);
        assert!(cache.len() <= 2);
        assert!(cache.get(b"c3", 3).is_some());
    }
}
