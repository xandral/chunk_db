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
