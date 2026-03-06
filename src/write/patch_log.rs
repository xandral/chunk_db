use std::collections::HashMap;
use std::sync::RwLock;
use arrow::record_batch::RecordBatch;

#[derive(Debug, Clone)]
pub enum PatchOp {
    Insert(RecordBatch),
    Update(RecordBatch),
    Delete(Vec<u64>),
}

#[derive(Debug, Clone)]
pub struct PatchEntry {
    pub tx_id: u64,
    pub op: PatchOp,
}

type ChunkKey = Vec<u8>;

pub struct PatchLog {
    patches: RwLock<HashMap<ChunkKey, Vec<PatchEntry>>>,
}

impl PatchLog {
    pub fn new() -> Self {
        Self {
            patches: RwLock::new(HashMap::new()),
        }
    }

    pub fn record(&self, chunk_key: &[u8], tx_id: u64, op: PatchOp) -> crate::Result<()> {
        let mut map = self.patches.write().unwrap();
        let entries = map.entry(chunk_key.to_vec()).or_default();
        let pos = entries.partition_point(|e| e.tx_id <= tx_id);
        entries.insert(pos, PatchEntry { tx_id, op });
        Ok(())
    }

    pub fn get_patches(&self, chunk_key: &[u8]) -> Vec<PatchEntry> {
        let map = self.patches.read().unwrap();
        map.get(chunk_key).cloned().unwrap_or_default()
    }

    pub fn get_patches_up_to(&self, chunk_key: &[u8], max_tx_id: u64) -> Vec<PatchEntry> {
        let map = self.patches.read().unwrap();
        map.get(chunk_key)
            .map(|entries| entries.iter().filter(|e| e.tx_id <= max_tx_id).cloned().collect())
            .unwrap_or_default()
    }

    pub fn get_patches_after(&self, chunk_key: &[u8], after_tx_id: u64) -> Vec<PatchEntry> {
        let map = self.patches.read().unwrap();
        map.get(chunk_key)
            .map(|entries| entries.iter().filter(|e| e.tx_id > after_tx_id).cloned().collect())
            .unwrap_or_default()
    }

    /// Get patches with tx_id in (after_tx, up_to_tx] — used for stale cache delta
    pub fn get_patches_between(&self, chunk_key: &[u8], after_tx: u64, up_to_tx: u64) -> Vec<PatchEntry> {
        let map = self.patches.read().unwrap();
        map.get(chunk_key)
            .map(|entries| entries.iter()
                .filter(|e| e.tx_id > after_tx && e.tx_id <= up_to_tx)
                .cloned().collect())
            .unwrap_or_default()
    }

    pub fn has_patches(&self, chunk_key: &[u8]) -> bool {
        let map = self.patches.read().unwrap();
        map.get(chunk_key).map_or(false, |v| !v.is_empty())
    }

    pub fn max_tx_id(&self, chunk_key: &[u8]) -> Option<u64> {
        let map = self.patches.read().unwrap();
        map.get(chunk_key)
            .and_then(|entries| entries.iter().map(|e| e.tx_id).max())
    }

    pub fn clear_patches(&self, chunk_key: &[u8]) {
        let mut map = self.patches.write().unwrap();
        map.remove(chunk_key);
    }

    pub fn clear_patches_up_to(&self, chunk_key: &[u8], max_tx: u64) {
        let mut map = self.patches.write().unwrap();
        if let Some(entries) = map.get_mut(chunk_key) {
            entries.retain(|e| e.tx_id > max_tx);
            if entries.is_empty() {
                map.remove(chunk_key);
            }
        }
    }

    pub fn dirty_chunks(&self) -> Vec<ChunkKey> {
        let map = self.patches.read().unwrap();
        map.keys().cloned().collect()
    }

    pub fn total_entries(&self) -> usize {
        let map = self.patches.read().unwrap();
        map.values().map(|v| v.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_test_batch(row_ids: Vec<u64>, values: Vec<i64>) -> RecordBatch {
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
    fn test_empty_log() {
        let log = PatchLog::new();
        assert!(!log.has_patches(b"chunk_1"));
        assert!(log.get_patches(b"chunk_1").is_empty());
        assert!(log.dirty_chunks().is_empty());
        assert_eq!(log.total_entries(), 0);
        assert_eq!(log.max_tx_id(b"chunk_1"), None);
    }

    #[test]
    fn test_record_insert() {
        let log = PatchLog::new();
        let batch = make_test_batch(vec![1, 2], vec![10, 20]);
        log.record(b"chunk_1", 1, PatchOp::Insert(batch)).unwrap();
        assert!(log.has_patches(b"chunk_1"));
        let patches = log.get_patches(b"chunk_1");
        assert_eq!(patches.len(), 1);
        assert!(matches!(patches[0].op, PatchOp::Insert(_)));
        assert_eq!(patches[0].tx_id, 1);
        assert_eq!(log.max_tx_id(b"chunk_1"), Some(1));
    }

    #[test]
    fn test_record_update() {
        let log = PatchLog::new();
        let batch = make_test_batch(vec![1], vec![999]);
        log.record(b"chunk_1", 2, PatchOp::Update(batch)).unwrap();
        let patches = log.get_patches(b"chunk_1");
        assert_eq!(patches.len(), 1);
        assert!(matches!(patches[0].op, PatchOp::Update(_)));
    }

    #[test]
    fn test_record_delete() {
        let log = PatchLog::new();
        log.record(b"chunk_1", 3, PatchOp::Delete(vec![5, 10])).unwrap();
        let patches = log.get_patches(b"chunk_1");
        assert_eq!(patches.len(), 1);
        assert!(matches!(patches[0].op, PatchOp::Delete(ref ids) if ids == &vec![5, 10]));
    }

    #[test]
    fn test_mixed_ops_ordered() {
        let log = PatchLog::new();
        let batch1 = make_test_batch(vec![1], vec![10]);
        let batch2 = make_test_batch(vec![1], vec![99]);
        log.record(b"c1", 3, PatchOp::Delete(vec![5])).unwrap();
        log.record(b"c1", 1, PatchOp::Insert(batch1)).unwrap();
        log.record(b"c1", 2, PatchOp::Update(batch2)).unwrap();
        let patches = log.get_patches(b"c1");
        assert_eq!(patches.len(), 3);
        assert_eq!(patches[0].tx_id, 1);
        assert_eq!(patches[1].tx_id, 2);
        assert_eq!(patches[2].tx_id, 3);
        assert_eq!(log.max_tx_id(b"c1"), Some(3));
    }

    #[test]
    fn test_get_patches_up_to() {
        let log = PatchLog::new();
        let b1 = make_test_batch(vec![1], vec![10]);
        let b2 = make_test_batch(vec![1], vec![20]);
        log.record(b"c1", 1, PatchOp::Insert(b1)).unwrap();
        log.record(b"c1", 5, PatchOp::Update(b2)).unwrap();
        log.record(b"c1", 10, PatchOp::Delete(vec![1])).unwrap();
        let patches = log.get_patches_up_to(b"c1", 5);
        assert_eq!(patches.len(), 2);
        assert_eq!(patches[0].tx_id, 1);
        assert_eq!(patches[1].tx_id, 5);
    }

    #[test]
    fn test_separate_chunks() {
        let log = PatchLog::new();
        log.record(b"c1", 1, PatchOp::Delete(vec![1])).unwrap();
        log.record(b"c2", 2, PatchOp::Delete(vec![2])).unwrap();
        assert_eq!(log.get_patches(b"c1").len(), 1);
        assert_eq!(log.get_patches(b"c2").len(), 1);
        assert_eq!(log.dirty_chunks().len(), 2);
        assert_eq!(log.total_entries(), 2);
    }

    #[test]
    fn test_clear_patches() {
        let log = PatchLog::new();
        log.record(b"c1", 1, PatchOp::Delete(vec![1])).unwrap();
        log.record(b"c2", 2, PatchOp::Delete(vec![2])).unwrap();
        log.record(b"c2", 5, PatchOp::Delete(vec![3])).unwrap();

        log.clear_patches(b"c1");
        assert!(!log.has_patches(b"c1"));
        assert!(log.has_patches(b"c2"));
        assert_eq!(log.max_tx_id(b"c1"), None);

        // clear_patches_up_to
        log.clear_patches_up_to(b"c2", 2);
        let patches = log.get_patches(b"c2");
        assert_eq!(patches.len(), 1);
        assert_eq!(patches[0].tx_id, 5);
    }
}
