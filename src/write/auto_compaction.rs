use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

pub struct AutoCompactionConfig {
    pub max_patches_per_chunk: usize,
    pub max_total_patches: usize,
    pub check_interval: Duration,
}

impl Default for AutoCompactionConfig {
    fn default() -> Self {
        Self {
            max_patches_per_chunk: 50,
            max_total_patches: 500,
            check_interval: Duration::from_secs(30),
        }
    }
}

pub struct CompactionHandle {
    shutdown: Arc<AtomicBool>,
    trigger: Arc<Notify>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl CompactionHandle {
    pub fn new(
        shutdown: Arc<AtomicBool>,
        trigger: Arc<Notify>,
        join_handle: tokio::task::JoinHandle<()>,
    ) -> Self {
        Self { shutdown, trigger, join_handle }
    }

    pub fn trigger_now(&self) {
        self.trigger.notify_one();
    }

    pub async fn shutdown(self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.trigger.notify_one();
        let _ = self.join_handle.await;
    }

    pub fn is_running(&self) -> bool {
        !self.join_handle.is_finished()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = AutoCompactionConfig::default();
        assert_eq!(config.max_patches_per_chunk, 50);
        assert_eq!(config.max_total_patches, 500);
        assert_eq!(config.check_interval, Duration::from_secs(30));
    }

    #[tokio::test]
    async fn test_shutdown() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let trigger = Arc::new(Notify::new());
        let s = shutdown.clone();
        let t = trigger.clone();
        let handle = CompactionHandle::new(
            shutdown,
            trigger,
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(3600)) => {},
                        _ = t.notified() => {},
                    }
                    if s.load(Ordering::Relaxed) {
                        break;
                    }
                }
            }),
        );
        assert!(handle.is_running());
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_trigger_now() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let trigger = Arc::new(Notify::new());
        let s = shutdown.clone();
        let t = trigger.clone();
        let handle = CompactionHandle::new(
            shutdown,
            trigger,
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(3600)) => {},
                        _ = t.notified() => {},
                    }
                    if s.load(Ordering::Relaxed) {
                        break;
                    }
                }
            }),
        );
        handle.trigger_now();
        // Just verify it doesn't panic
        handle.shutdown().await;
    }
}
