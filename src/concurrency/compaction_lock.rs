use std::collections::HashSet;
use std::sync::RwLock;

pub struct CompactionLock {
    locks: RwLock<HashSet<Vec<u8>>>,
}

impl CompactionLock {
    pub fn new() -> Self {
        Self {
            locks: RwLock::new(HashSet::new()),
        }
    }

    pub fn try_lock(&self, chunk_key: &[u8]) -> bool {
        let mut set = self.locks.write().unwrap();
        set.insert(chunk_key.to_vec())
    }

    pub fn unlock(&self, chunk_key: &[u8]) {
        let mut set = self.locks.write().unwrap();
        set.remove(chunk_key);
    }

    /// RAII variant of try_lock: the returned guard unlocks on drop (also on
    /// early returns and errors). None = someone else holds the key.
    pub fn try_acquire(&self, chunk_key: &[u8]) -> Option<CompactionGuard<'_>> {
        if self.try_lock(chunk_key) {
            Some(CompactionGuard { lock: self, key: chunk_key.to_vec() })
        } else {
            None
        }
    }
}

pub struct CompactionGuard<'a> {
    lock: &'a CompactionLock,
    key: Vec<u8>,
}

impl Drop for CompactionGuard<'_> {
    fn drop(&mut self) {
        self.lock.unlock(&self.key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_unlock() {
        let lock = CompactionLock::new();
        assert!(lock.try_lock(b"chunk_1"));
        lock.unlock(b"chunk_1");
        assert!(lock.try_lock(b"chunk_1"));
    }

    #[test]
    fn test_double_lock_fails() {
        let lock = CompactionLock::new();
        assert!(lock.try_lock(b"chunk_1"));
        assert!(!lock.try_lock(b"chunk_1"));
    }

    #[test]
    fn test_different_chunks() {
        let lock = CompactionLock::new();
        assert!(lock.try_lock(b"chunk_1"));
        assert!(lock.try_lock(b"chunk_2"));
    }

    #[test]
    fn test_unlock_then_relock() {
        let lock = CompactionLock::new();
        assert!(lock.try_lock(b"chunk_1"));
        lock.unlock(b"chunk_1");
        assert!(lock.try_lock(b"chunk_1"));
    }
}
