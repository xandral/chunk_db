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

/// Maximum refinement level. Beyond this, 2^level shifts stop being meaningful
/// for u64 row-id space (and cells are long since width-1).
pub const MAX_SPLIT_LEVEL: u16 = 63;

/// Row bucket at a refinement level: floor(row_id * 2^level / base_width).
///
/// Computed in u128 so it is exact for any base_width (not just powers of two).
/// At level 0 this is exactly `row_id / base_width` (v0 behavior). If
/// bucket_at_level(r, W, L) = b, then bucket_at_level(r, W, L+1) ∈ {2b, 2b+1}:
/// cell (L, b) splits into exactly (L+1, 2b) and (L+1, 2b+1).
pub fn bucket_at_level(row_id: u64, base_width: u64, level: u16) -> u64 {
    debug_assert!(level <= MAX_SPLIT_LEVEL);
    // Clamp defensively: splits stop once cells reach width 1 (2^level ~ W),
    // so real quotients always fit in u64.
    ((row_id as u128) << level)
        .div_euclid(base_width as u128)
        .min(u64::MAX as u128) as u64
}

/// Row-id interval [start, end) covered by cell (level, bucket).
///
/// Inverse of bucket_at_level: start = ceil(b*W / 2^level),
/// end = ceil((b+1)*W / 2^level), clamped to u64 range.
pub fn cell_row_range(bucket: u64, base_width: u64, level: u16) -> (u64, u64) {
    let w = base_width as u128;
    let ceil_div = |num: u128, shift: u16| -> u64 {
        let d = 1u128 << shift;
        ((num + d - 1) / d).min(u64::MAX as u128) as u64
    };
    let start = ceil_div(bucket as u128 * w, level);
    let end = ceil_div((bucket as u128 + 1) * w, level);
    (start, end)
}

/// A cell can split only if its row-id interval spans at least 2 values,
/// the level cap is not reached, and both child bucket indices (2b, 2b+1)
/// still fit in u64.
pub fn cell_is_splittable(bucket: u64, base_width: u64, level: u16) -> bool {
    if level >= MAX_SPLIT_LEVEL || bucket > (u64::MAX - 1) / 2 {
        return false;
    }
    let (start, end) = cell_row_range(bucket, base_width, level);
    end.saturating_sub(start) >= 2
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
    fn test_bucket_at_level_zero_matches_v0() {
        for &w in &[1u64, 3, 100, 100_000, u64::MAX] {
            for &r in &[0u64, 1, 99, 100_000, u64::MAX / 2, u64::MAX] {
                assert_eq!(bucket_at_level(r, w, 0), r / w, "r={} w={}", r, w);
            }
        }
    }

    #[test]
    fn test_child_relation() {
        // Only levels reachable by splitting matter: a cell splits while its
        // width is >= 2, i.e. 2^(level+1) <= W. cell_is_splittable also
        // guards against child buckets overflowing u64.
        let w = 100_000u64;
        for &r in &[0u64, 1, 49_999, 50_000, 99_999, 12_345_678, u64::MAX] {
            for level in 0..20u16 {
                if (1u128 << (level + 1)) > w as u128 {
                    break;
                }
                let b = bucket_at_level(r, w, level);
                if !cell_is_splittable(b, w, level) {
                    continue;
                }
                let child = bucket_at_level(r, w, level + 1) as u128;
                let b = b as u128;
                assert!(child == 2 * b || child == 2 * b + 1,
                    "r={} level={} b={} child={}", r, level, b, child);
            }
        }
    }

    #[test]
    fn test_cell_row_range_roundtrip() {
        let w = 100_001u64; // odd width: exercises non-power-of-two division
        for level in 0..8u16 {
            for bucket in 0..20u64 {
                let (start, end) = cell_row_range(bucket, w, level);
                if start >= end {
                    continue; // empty cell at this level
                }
                assert_eq!(bucket_at_level(start, w, level), bucket);
                assert_eq!(bucket_at_level(end - 1, w, level), bucket);
                if start > 0 {
                    assert_ne!(bucket_at_level(start - 1, w, level), bucket);
                }
                assert_ne!(bucket_at_level(end, w, level), bucket);
            }
        }
    }

    #[test]
    fn test_cell_is_splittable() {
        // Width-4 base cell: splittable down to width 1
        assert!(cell_is_splittable(0, 4, 0));
        assert!(cell_is_splittable(0, 4, 1));
        assert!(!cell_is_splittable(0, 4, 2)); // width 1
        // Width-1 cells never split
        assert!(!cell_is_splittable(5, 1, 0));
        // Level cap
        assert!(!cell_is_splittable(0, u64::MAX, MAX_SPLIT_LEVEL));
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
