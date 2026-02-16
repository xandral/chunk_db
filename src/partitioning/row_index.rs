/// Convert i64 to u64 preserving total ordering.
///
/// Uses XOR with the sign bit so that:
///   i64::MIN → 0, -1 → 2^63-1, 0 → 2^63, i64::MAX → u64::MAX
///
/// This ensures negative primary keys map to lower row buckets than
/// positive ones, enabling correct row bucket pruning across the
/// entire i64 range.
pub fn i64_to_ordered_u64(v: i64) -> u64 {
    (v as u64) ^ (1u64 << 63)
}

/// Calculate row bucket from row ID
pub fn row_bucket(row_id: u64, chunk_rows: u64) -> u64 {
    row_id / chunk_rows
}

/// Calculate row range for a bucket
pub fn bucket_row_range(bucket: u64, chunk_rows: u64) -> (u64, u64) {
    let start = bucket * chunk_rows;
    let end = start + chunk_rows;
    (start, end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_i64_to_ordered_u64_preserves_order() {
        assert!(i64_to_ordered_u64(i64::MIN) < i64_to_ordered_u64(-1));
        assert!(i64_to_ordered_u64(-1) < i64_to_ordered_u64(0));
        assert!(i64_to_ordered_u64(0) < i64_to_ordered_u64(1));
        assert!(i64_to_ordered_u64(1) < i64_to_ordered_u64(i64::MAX));
    }

    #[test]
    fn test_i64_to_ordered_u64_boundaries() {
        assert_eq!(i64_to_ordered_u64(i64::MIN), 0);
        assert_eq!(i64_to_ordered_u64(i64::MAX), u64::MAX);
    }

    #[test]
    fn test_negative_values_contiguous_buckets() {
        let chunk_rows = 100_000u64;
        // -1 and -100_000 should be in the same or adjacent bucket
        let b1 = row_bucket(i64_to_ordered_u64(-1), chunk_rows);
        let b2 = row_bucket(i64_to_ordered_u64(-100_000), chunk_rows);
        assert!(b1 - b2 <= 1, "Negative timestamps should map to nearby buckets");
    }
}
