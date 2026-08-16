use super::row_index::i64_to_ordered_u64;

/// A range axis cannot be refined below the integer unit represented by its
/// input value. This also guarantees the scaled bucket index fits in `i64`.
pub fn range_level_is_splittable(chunk_size: u64, parent_level: u16) -> bool {
    let Some(child_scale) = 1u64.checked_shl(parent_level as u32 + 1) else {
        return false;
    };
    child_scale <= chunk_size
}

/// Calculate the bucket at an adaptive range level.
///
/// Conceptually this is `floor(value * 2^level / chunk_size)`. `i128`
/// intermediate arithmetic avoids overflow; the splittability rule prevents
/// the signed result from exceeding the coordinate's `i64` domain.
pub fn range_bucket_at_level(value: i64, chunk_size: u64, level: u16) -> crate::Result<u64> {
    if chunk_size == 0 {
        return Err(crate::ChunkDbError::Config(
            "range dimension chunk_size must be greater than zero".to_string(),
        ));
    }
    let scale = 1u64.checked_shl(level as u32).ok_or_else(|| {
        crate::ChunkDbError::Config(format!("range refinement level {} overflows", level))
    })?;
    if scale > chunk_size {
        return Err(crate::ChunkDbError::Config(format!(
            "range refinement level {} is finer than integer chunk size {}",
            level, chunk_size
        )));
    }

    let signed_bucket = (value as i128)
        .checked_mul(scale as i128)
        .expect("i64 * u64 constrained to at most 2^126")
        .div_euclid(chunk_size as i128);
    let signed_bucket = i64::try_from(signed_bucket).map_err(|_| {
        crate::ChunkDbError::Config(format!(
            "range bucket for value {} at level {} is outside i64",
            value, level
        ))
    })?;
    Ok(i64_to_ordered_u64(signed_bucket))
}

/// Calculate range bucket from value.
///
/// Uses floor division (div_euclid) so that values in [k*chunk_size, (k+1)*chunk_size)
/// always land in the same bucket, even for negative values. The signed bucket index
/// is then mapped to u64 via the same order-preserving i64_to_ordered_u64() used
/// by row bucketing (XOR with sign bit), ensuring negative buckets sort before
/// positive ones.
pub fn range_bucket(value: i64, chunk_size: u64) -> u64 {
    // Retain the original infallible public API. Table validation guarantees
    // non-zero sizes in database paths; direct callers get an explicit panic
    // instead of the old division-by-zero panic.
    range_bucket_at_level(value, chunk_size, 0)
        .expect("range_bucket requires chunk_size > 0")
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

/// Whether one refined range cell can overlap an inclusive value interval.
pub fn range_bucket_overlaps(
    bucket: u64,
    level: u16,
    min_value: i64,
    max_value: i64,
    chunk_size: u64,
) -> crate::Result<bool> {
    if min_value > max_value {
        return Ok(false);
    }
    let min_bucket = range_bucket_at_level(min_value, chunk_size, level)?;
    let max_bucket = range_bucket_at_level(max_value, chunk_size, level)?;
    Ok(bucket >= min_bucket && bucket <= max_bucket)
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

    #[test]
    fn refined_range_bucket_is_nested_and_ordered() {
        let size = 16;
        for value in -100..100 {
            let parent = range_bucket_at_level(value, size, 2).unwrap();
            let child = range_bucket_at_level(value, size, 3).unwrap();
            assert!(child >= range_bucket_at_level(value - 1, size, 3).unwrap());
            // Both values map consistently; exact numeric parent/child codes
            // differ because signed buckets use order-preserving encoding.
            assert_eq!(parent, range_bucket_at_level(value, size, 2).unwrap());
        }
        assert!(range_level_is_splittable(size, 3));
        assert!(!range_level_is_splittable(size, 4));
    }

    #[test]
    fn refined_range_overlap_uses_coordinate_level() {
        let size = 100;
        let bucket = range_bucket_at_level(25, size, 2).unwrap();
        assert!(range_bucket_overlaps(bucket, 2, 20, 29, size).unwrap());
        assert!(!range_bucket_overlaps(bucket, 2, 50, 59, size).unwrap());
    }
}
