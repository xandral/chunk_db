use std::collections::{HashMap, HashSet};

use crate::catalog::{HashRegistry, VersionCatalog, RangeDimensionStats};
use crate::config::table_config::{TableConfig, RowIdStrategy};
use crate::partitioning::{
    cell_row_range, i64_to_ordered_u64, range_bucket_overlaps, ColumnGroupMapper,
};
use crate::query::filter::{Filter, FilterOp, FilterValue};
use crate::storage::ChunkInfo;
use crate::Result;

/// Extract range predicates from filters
pub struct PredicateExtractor<'a> {
    config: &'a TableConfig,
}

impl<'a> PredicateExtractor<'a> {
    pub fn new(config: &'a TableConfig) -> Self {
        Self { config }
    }

    /// Extract (column, min, max) for range dimensions
    pub fn extract_range_predicates(&self, filters: &[Filter]) -> Vec<(String, i64, i64)> {
        let mut ranges: Vec<(String, i64, i64)> = vec![];

        for dim in &self.config.partitioning.range_dimensions {
            let mut min_val = i64::MIN;
            let mut max_val = i64::MAX;

            for filter in filters {
                if filter.column == dim.column {
                    if let FilterValue::Int(val) = filter.value {
                        match filter.op {
                            FilterOp::Gt => min_val = min_val.max(val + 1),
                            FilterOp::GtEq => min_val = min_val.max(val),
                            FilterOp::Lt => max_val = max_val.min(val - 1),
                            FilterOp::LtEq => max_val = max_val.min(val),
                            FilterOp::Eq => {
                                min_val = val;
                                max_val = val;
                            }
                            _ => {}
                        }
                    }
                }
            }

            if min_val != i64::MIN || max_val != i64::MAX {
                ranges.push((dim.column.clone(), min_val, max_val));
            }
        }

        ranges
    }

    /// Extract equality predicates for hash dimensions
    /// Returns (dimension_index, column_name, FilterValue) tuples preserving
    /// both type information and the configured dimension position.
    pub fn extract_hash_predicates(
        &self,
        filters: &[Filter],
    ) -> Vec<(usize, String, FilterValue)> {
        let mut predicates = vec![];

        for (dim_idx, dim) in self.config.partitioning.hash_dimensions.iter().enumerate() {
            for filter in filters {
                if filter.column == dim.column && filter.op == FilterOp::Eq {
                    predicates.push((dim_idx, dim.column.clone(), filter.value.clone()));
                    break;
                }
            }
        }

        predicates
    }
}

/// Prune chunks based on predicates and projection
pub fn prune_chunks(
    config: &TableConfig,
    catalog: &VersionCatalog,
    hash_registries: &[HashRegistry],
    column_mapper: &ColumnGroupMapper,
    filters: &[Filter],
    projection: Option<&Vec<usize>>,
    schema: &arrow::datatypes::Schema,
) -> Result<Vec<ChunkInfo>> {
    // 1. Get all chunks from catalog for this table
    let all_chunks = catalog.all_chunks(&config.name)?;
    let mut candidates: Vec<ChunkInfo> = all_chunks.into_iter()
        .map(|(coord, version)| ChunkInfo { coord, version })
        .collect();

    let extractor = PredicateExtractor::new(config);

    // Load range stats for bounding unbounded queries
    let range_stats = catalog.get_all_range_stats(&config.name)?;
    let stats_map: HashMap<String, &RangeDimensionStats> = range_stats.iter()
        .map(|s| (s.column.clone(), s))
        .collect();

    // 2. Row bucket pruning (implicit range dimension based on primary key)
    // When row_id_strategy = SingleColumn(col), row_bucket = col_value / chunk_rows
    if let RowIdStrategy::SingleColumn(ref pk_col) = config.row_id_strategy {
        // Extract range predicate for the primary key column
        let mut min_val = i64::MIN;
        let mut max_val = i64::MAX;

        for filter in filters {
            if &filter.column == pk_col {
                if let FilterValue::Int(val) = filter.value {
                    match filter.op {
                        FilterOp::Gt => min_val = min_val.max(val + 1),
                        FilterOp::GtEq => min_val = min_val.max(val),
                        FilterOp::Lt => max_val = max_val.min(val - 1),
                        FilterOp::LtEq => max_val = max_val.min(val),
                        FilterOp::Eq => {
                            min_val = val;
                            max_val = val;
                        }
                        _ => {}
                    }
                }
            }
        }

        // If we have bounds, prune by interval overlap between the cell's
        // row-id range (level-aware) and the predicate range, using the
        // order-preserving i64→u64 mapping. At level 0 the cell range is
        // [b*chunk_rows, (b+1)*chunk_rows) — identical to v0 bucket pruning.
        if min_val != i64::MIN || max_val != i64::MAX {
            let chunk_rows = config.partitioning.chunk_rows;

            let min_u = if min_val == i64::MIN { 0 } else { i64_to_ordered_u64(min_val) };
            let max_u = if max_val == i64::MAX { u64::MAX } else { i64_to_ordered_u64(max_val) };

            candidates.retain(|chunk| {
                let (start, end) =
                    cell_row_range(chunk.coord.row_bucket, chunk_rows, chunk.coord.level);
                // overlap of [start, end) with [min_u, max_u]; a clamped end
                // (u64::MAX) is treated as inclusive so the last cell of the
                // row-id space is never pruned away (over-inclusion is safe).
                start <= max_u && (end > min_u || end == u64::MAX)
            });
        }
    }

    // 3. Range dimension pruning (explicit range dimensions)
    let range_predicates = extractor.extract_range_predicates(filters);
    for (col_name, min_val, max_val) in range_predicates.iter() {
        // Find the dimension index by column name
        let Some((dim_idx, dim)) = config.partitioning.range_dimensions.iter()
            .enumerate()
            .find(|(_, d)| &d.column == col_name) else {
            continue;
        };

        // Replace unbounded values with actual data bounds
        let (effective_min, effective_max) = bound_range_with_stats(
            *min_val,
            *max_val,
            stats_map.get(col_name),
        );

        // Statistics did not provide finite bounds: over-inclusion is the
        // only safe choice. Otherwise compare at each coordinate's own level.
        if effective_min == i64::MIN || effective_max == i64::MAX {
            continue;
        }
        let mut filtered = Vec::with_capacity(candidates.len());
        for chunk in candidates {
            if let Some(&bucket) = chunk.coord.range_buckets.get(dim_idx) {
                let level = chunk.coord.range_levels.get(dim_idx).copied().unwrap_or(0);
                if range_bucket_overlaps(
                    bucket,
                    level,
                    effective_min,
                    effective_max,
                    dim.chunk_size,
                )? {
                    filtered.push(chunk);
                }
            } else {
                // Legacy/malformed coordinates are conservatively retained.
                filtered.push(chunk);
            }
        }
        candidates = filtered;
    }

    // 4. Hash dimension pruning
    let hash_predicates = extractor.extract_hash_predicates(filters);
    for (dim_idx, _col_name, value) in &hash_predicates {
        if *dim_idx >= hash_registries.len() {
            continue;
        }

        let mut bucket_by_level = HashMap::new();
        for chunk in &candidates {
            let level = chunk.coord.hash_levels.get(*dim_idx).copied().unwrap_or(0);
            if bucket_by_level.contains_key(&level) {
                continue;
            }
            let bucket = match value {
                FilterValue::String(s) => {
                    hash_registries[*dim_idx].lookup_bucket_at_level(s, level)?
                }
                FilterValue::Int(n) => {
                    hash_registries[*dim_idx].lookup_bucket_numeric_at_level(*n, level)?
                }
                FilterValue::UInt(n) => {
                    hash_registries[*dim_idx]
                        .lookup_bucket_numeric_at_level(*n as i64, level)?
                }
                FilterValue::Bool(value) => {
                    hash_registries[*dim_idx].lookup_bucket_bool_at_level(*value, level)?
                }
            };
            bucket_by_level.insert(level, bucket);
        }
        candidates.retain(|chunk| {
            let Some(&actual) = chunk.coord.hash_buckets.get(*dim_idx) else {
                return true;
            };
            let level = chunk.coord.hash_levels.get(*dim_idx).copied().unwrap_or(0);
            bucket_by_level.get(&level).copied() == Some(actual)
        });
    }

    // 5. Column projection pruning
    let required_groups: HashSet<u16> = if let Some(proj) = projection {
        let required_columns: Vec<String> = proj.iter()
            .map(|&i| schema.field(i).name().clone())
            .collect();
        column_mapper.required_groups(&required_columns)
            .into_iter()
            .collect()
    } else {
        (0..column_mapper.num_groups()).collect()
    };

    candidates.retain(|chunk| required_groups.contains(&chunk.coord.col_group));

    // 6. Version resolution
    let mut latest_by_coord: HashMap<crate::storage::ChunkCoordinate, ChunkInfo> = HashMap::new();

    for chunk in candidates {
        let key = chunk.coord.clone();

        latest_by_coord
            .entry(key)
            .and_modify(|existing| {
                if chunk.version > existing.version {
                    *existing = chunk.clone();
                }
            })
            .or_insert(chunk);
    }

    Ok(latest_by_coord.into_values().collect())
}

/// Replace i64::MIN/MAX with actual data bounds from statistics
fn bound_range_with_stats(
    min_val: i64,
    max_val: i64,
    stats: Option<&&RangeDimensionStats>,
) -> (i64, i64) {
    let Some(stats) = stats else {
        // No stats available, keep original bounds
        return (min_val, max_val);
    };

    if !stats.is_initialized() {
        // No data inserted yet
        return (min_val, max_val);
    }

    let effective_min = if min_val == i64::MIN {
        stats.min_value
    } else {
        min_val
    };

    let effective_max = if max_val == i64::MAX {
        stats.max_value
    } else {
        max_val
    };

    (effective_min, effective_max)
}
