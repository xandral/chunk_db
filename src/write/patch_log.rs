use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, RwLock};
use arrow::record_batch::RecordBatch;

use super::patch_wal::{PatchWal, WalRecord};

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
    /// Write-ahead log: every mutation is appended (and fsynced) here before
    /// touching the in-memory map. None = volatile log (v0 behavior, tests).
    /// Lock order is always `patches` then `wal`.
    wal: Option<Mutex<PatchWal>>,
}

impl PatchLog {
    /// Volatile log: patches are lost on crash. Kept for tests and embedded
    /// use cases that explicitly opt out of durability.
    pub fn new() -> Self {
        Self {
            patches: RwLock::new(HashMap::new()),
            wal: None,
        }
    }

    /// Durable log: replay the WAL at `path` into memory, then checkpoint it
    /// (rewrite from live state) so superseded records don't accumulate
    /// across restarts.
    pub fn with_wal(path: &Path) -> crate::Result<Self> {
        let (wal, records) = PatchWal::open(path)?;

        let mut map: HashMap<ChunkKey, Vec<PatchEntry>> = HashMap::new();
        for record in records {
            match record {
                WalRecord::Patch { key, entry } => {
                    let entries = map.entry(key).or_default();
                    let pos = entries.partition_point(|e| e.tx_id <= entry.tx_id);
                    entries.insert(pos, entry);
                }
                WalRecord::Clear { key } => {
                    map.remove(&key);
                }
                WalRecord::ClearUpTo { key, max_tx } => {
                    if let Some(entries) = map.get_mut(&key) {
                        entries.retain(|e| e.tx_id > max_tx);
                        if entries.is_empty() {
                            map.remove(&key);
                        }
                    }
                }
            }
        }

        let log = Self {
            patches: RwLock::new(map),
            wal: Some(Mutex::new(wal)),
        };
        log.checkpoint()?;
        Ok(log)
    }

    /// Rewrite the WAL from the live in-memory entries (drops superseded
    /// records). No-op for volatile logs.
    pub fn checkpoint(&self) -> crate::Result<()> {
        let map = self.patches.write().unwrap();
        if let Some(wal) = &self.wal {
            let live = map.iter()
                .flat_map(|(key, entries)| entries.iter().map(move |e| (key.as_slice(), e)));
            wal.lock().unwrap().rewrite(live)?;
        }
        Ok(())
    }

    /// Truncate the WAL when the in-memory log just became empty — the
    /// common lifecycle end (compaction or split cleared everything).
    fn maybe_truncate(&self, map: &HashMap<ChunkKey, Vec<PatchEntry>>) -> crate::Result<()> {
        if map.is_empty() {
            if let Some(wal) = &self.wal {
                wal.lock().unwrap().rewrite(std::iter::empty())?;
            }
        }
        Ok(())
    }

    pub fn record(&self, chunk_key: &[u8], tx_id: u64, op: PatchOp) -> crate::Result<()> {
        let mut map = self.patches.write().unwrap();
        // WAL first: if the append (or its fsync) fails, the patch is neither
        // durable nor visible.
        if let Some(wal) = &self.wal {
            wal.lock().unwrap().append_patch(chunk_key, tx_id, &op)?;
        }
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

    pub fn clear_patches(&self, chunk_key: &[u8]) -> crate::Result<()> {
        let mut map = self.patches.write().unwrap();
        if map.remove(chunk_key).is_some() {
            if let Some(wal) = &self.wal {
                wal.lock().unwrap().append_clear(chunk_key)?;
            }
            self.maybe_truncate(&map)?;
        }
        Ok(())
    }

    /// Atomically remove and return all patches for a key. Unlike a
    /// get-then-clear pair, no patch recorded in between can be dropped
    /// unseen — the split uses this to re-route in-flight patches.
    pub fn drain(&self, chunk_key: &[u8]) -> crate::Result<Vec<PatchEntry>> {
        let mut map = self.patches.write().unwrap();
        let entries = map.remove(chunk_key).unwrap_or_default();
        if !entries.is_empty() {
            if let Some(wal) = &self.wal {
                wal.lock().unwrap().append_clear(chunk_key)?;
            }
            self.maybe_truncate(&map)?;
        }
        Ok(entries)
    }

    pub fn clear_patches_up_to(&self, chunk_key: &[u8], max_tx: u64) -> crate::Result<()> {
        let mut map = self.patches.write().unwrap();
        if let Some(entries) = map.get_mut(chunk_key) {
            if let Some(wal) = &self.wal {
                wal.lock().unwrap().append_clear_up_to(chunk_key, max_tx)?;
            }
            entries.retain(|e| e.tx_id > max_tx);
            if entries.is_empty() {
                map.remove(chunk_key);
            }
            self.maybe_truncate(&map)?;
        }
        Ok(())
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

        log.clear_patches(b"c1").unwrap();
        assert!(!log.has_patches(b"c1"));
        assert!(log.has_patches(b"c2"));
        assert_eq!(log.max_tx_id(b"c1"), None);

        // clear_patches_up_to
        log.clear_patches_up_to(b"c2", 2).unwrap();
        let patches = log.get_patches(b"c2");
        assert_eq!(patches.len(), 1);
        assert_eq!(patches[0].tx_id, 5);
    }

    fn assert_same_entries(a: &[PatchEntry], b: &[PatchEntry]) {
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(b) {
            assert_eq!(x.tx_id, y.tx_id);
            match (&x.op, &y.op) {
                (PatchOp::Delete(i), PatchOp::Delete(j)) => assert_eq!(i, j),
                (PatchOp::Update(p), PatchOp::Update(q))
                | (PatchOp::Insert(p), PatchOp::Insert(q)) => assert_eq!(p, q),
                _ => panic!("op kind mismatch"),
            }
        }
    }

    #[test]
    fn test_wal_replay_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("patches.wal");

        let log = PatchLog::with_wal(&wal_path).unwrap();
        log.record(b"c1", 1, PatchOp::Update(make_test_batch(vec![1], vec![10]))).unwrap();
        log.record(b"c1", 2, PatchOp::Delete(vec![7, 9])).unwrap();
        log.record(b"c2", 3, PatchOp::Insert(make_test_batch(vec![4], vec![40]))).unwrap();
        let before_c1 = log.get_patches(b"c1");
        let before_c2 = log.get_patches(b"c2");
        drop(log);

        let reopened = PatchLog::with_wal(&wal_path).unwrap();
        assert_same_entries(&reopened.get_patches(b"c1"), &before_c1);
        assert_same_entries(&reopened.get_patches(b"c2"), &before_c2);
        assert_eq!(reopened.total_entries(), 3);
    }

    #[test]
    fn test_wal_replays_clears() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("patches.wal");

        let log = PatchLog::with_wal(&wal_path).unwrap();
        log.record(b"c1", 1, PatchOp::Delete(vec![1])).unwrap();
        log.record(b"c1", 5, PatchOp::Delete(vec![2])).unwrap();
        log.record(b"c2", 2, PatchOp::Delete(vec![3])).unwrap();
        log.clear_patches_up_to(b"c1", 1).unwrap();
        log.clear_patches(b"c2").unwrap();
        drop(log);

        let reopened = PatchLog::with_wal(&wal_path).unwrap();
        let c1 = reopened.get_patches(b"c1");
        assert_eq!(c1.len(), 1);
        assert_eq!(c1[0].tx_id, 5);
        assert!(!reopened.has_patches(b"c2"));
    }

    #[test]
    fn test_wal_truncates_when_emptied_and_survives_corrupt_tail() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("patches.wal");

        let log = PatchLog::with_wal(&wal_path).unwrap();
        log.record(b"c1", 1, PatchOp::Delete(vec![1])).unwrap();
        log.clear_patches(b"c1").unwrap();
        assert_eq!(
            std::fs::metadata(&wal_path).unwrap().len(), 0,
            "WAL must be truncated once the log is empty"
        );

        // Simulate a crash-truncated tail: valid record + garbage suffix.
        log.record(b"c1", 2, PatchOp::Delete(vec![9])).unwrap();
        drop(log);
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(&wal_path).unwrap();
        f.write_all(&[42u8, 7, 0, 99]).unwrap();
        drop(f);

        let reopened = PatchLog::with_wal(&wal_path).unwrap();
        let c1 = reopened.get_patches(b"c1");
        assert_eq!(c1.len(), 1, "records before the corrupt tail must survive");
        assert_eq!(c1[0].tx_id, 2);
    }
}
