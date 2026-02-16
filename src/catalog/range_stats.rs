use serde::{Deserialize, Serialize};

/// Statistics for a range dimension
///
/// Tracks the global minimum and maximum values observed for a column,
/// enabling efficient pruning of one-sided range queries (e.g., `timestamp > X`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeDimensionStats {
    /// Column name
    pub column: String,
    /// Minimum value ever inserted
    pub min_value: i64,
    /// Maximum value ever inserted
    pub max_value: i64,
    /// Number of values observed
    pub count: u64,
}

impl RangeDimensionStats {
    /// Create new uninitialized stats
    pub fn new(column: &str) -> Self {
        Self {
            column: column.to_string(),
            min_value: i64::MAX,  // Will be updated on first insert
            max_value: i64::MIN,  // Will be updated on first insert
            count: 0,
        }
    }

    /// Update stats with a new batch of values
    pub fn update(&mut self, min: i64, max: i64, count: u64) {
        self.min_value = self.min_value.min(min);
        self.max_value = self.max_value.max(max);
        self.count += count;
    }

    /// Check if stats have been initialized with data
    pub fn is_initialized(&self) -> bool {
        self.count > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_stats_new() {
        let stats = RangeDimensionStats::new("timestamp");
        assert_eq!(stats.column, "timestamp");
        assert!(!stats.is_initialized());
    }

    #[test]
    fn test_range_stats_update() {
        let mut stats = RangeDimensionStats::new("timestamp");

        // Initial state
        assert!(!stats.is_initialized());

        // First batch
        stats.update(100, 200, 10);
        assert!(stats.is_initialized());
        assert_eq!(stats.min_value, 100);
        assert_eq!(stats.max_value, 200);
        assert_eq!(stats.count, 10);

        // Second batch expands range
        stats.update(50, 300, 5);
        assert_eq!(stats.min_value, 50);  // New min
        assert_eq!(stats.max_value, 300); // New max
        assert_eq!(stats.count, 15);

        // Third batch is within range
        stats.update(100, 200, 5);
        assert_eq!(stats.min_value, 50);  // Unchanged
        assert_eq!(stats.max_value, 300); // Unchanged
        assert_eq!(stats.count, 20);
    }

    #[test]
    fn test_range_stats_negative_values() {
        let mut stats = RangeDimensionStats::new("temperature");

        stats.update(-100, -50, 10);
        assert_eq!(stats.min_value, -100);
        assert_eq!(stats.max_value, -50);

        stats.update(-200, 100, 10);
        assert_eq!(stats.min_value, -200);
        assert_eq!(stats.max_value, 100);
    }
}
