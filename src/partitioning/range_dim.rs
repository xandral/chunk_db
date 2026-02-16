use super::row_index::i64_to_ordered_u64;

/// Calculate range bucket from value.
///
/// Uses floor division (div_euclid) so that values in [k*chunk_size, (k+1)*chunk_size)
/// always land in the same bucket, even for negative values. The signed bucket index
/// is then mapped to u64 via the same order-preserving i64_to_ordered_u64() used
/// by row bucketing (XOR with sign bit), ensuring negative buckets sort before
/// positive ones.
pub fn range_bucket(value: i64, chunk_size: u64) -> u64 {
    let signed_bucket = value.div_euclid(chunk_size as i64);
    i64_to_ordered_u64(signed_bucket)
}

/// Maximum number of buckets we'll enumerate.
/// If a range spans more buckets than this, we skip range pruning.
const MAX_BUCKETS_TO_ENUMERATE: u64 = 10_000;

/// Find buckets that overlap with a value range.
///
/// Handles both positive and negative ranges correctly.
/// Returns None if the range is too large (unbounded) to enumerate efficiently.
pub fn overlapping_buckets(min_value: i64, max_value: i64, chunk_size: u64) -> Option<Vec<u64>> {
    // Handle unbounded ranges - skip pruning
    if min_value == i64::MIN || max_value == i64::MAX {
        return None;
    }

    let min_bucket = range_bucket(min_value, chunk_size);
    let max_bucket = range_bucket(max_value, chunk_size);

    // Avoid allocating huge vectors for very large ranges
    if max_bucket.saturating_sub(min_bucket) > MAX_BUCKETS_TO_ENUMERATE {
        return None;
    }

    Some((min_bucket..=max_bucket).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_positive_range_bucket() {
        assert_eq!(range_bucket(0, 1000), range_bucket(999, 1000));
        assert_ne!(range_bucket(999, 1000), range_bucket(1000, 1000));
        assert_eq!(range_bucket(1000, 1000), range_bucket(1999, 1000));
    }

    #[test]
    fn test_negative_range_bucket() {
        // -1 should be in a different bucket than 0
        assert_ne!(range_bucket(-1, 1000), range_bucket(0, 1000));

        // -1000 and -1 should be in same bucket (div_euclid: [-1000, 0))
        assert_eq!(range_bucket(-1000, 1000), range_bucket(-1, 1000));

        // -1001 should be in a different bucket than -1
        assert_ne!(range_bucket(-1001, 1000), range_bucket(-1, 1000));
    }

    #[test]
    fn test_ordering_preserved() {
        // Negative values should produce lower bucket numbers than positive
        assert!(range_bucket(-1000, 1000) < range_bucket(0, 1000));
        assert!(range_bucket(-1, 1000) < range_bucket(0, 1000));
        assert!(range_bucket(0, 1000) < range_bucket(1000, 1000));
    }

    #[test]
    fn test_overlapping_buckets_positive() {
        let buckets = overlapping_buckets(500, 1500, 1000).unwrap();
        assert_eq!(buckets.len(), 2);
    }

    #[test]
    fn test_overlapping_buckets_negative() {
        let buckets = overlapping_buckets(-1500, -500, 1000).unwrap();
        assert_eq!(buckets.len(), 2);
    }

    #[test]
    fn test_overlapping_buckets_cross_zero() {
        let buckets = overlapping_buckets(-500, 500, 1000).unwrap();
        assert_eq!(buckets.len(), 2);
    }

    #[test]
    fn test_overlapping_buckets_unbounded() {
        // Unbounded ranges should return None
        assert!(overlapping_buckets(i64::MIN, 1000, 1000).is_none());
        assert!(overlapping_buckets(0, i64::MAX, 1000).is_none());
        assert!(overlapping_buckets(i64::MIN, i64::MAX, 1000).is_none());
    }
}
