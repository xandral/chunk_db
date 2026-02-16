use std::collections::{HashMap, HashSet};

use crate::catalog::{HashRegistry, VersionCatalog, RangeDimensionStats};
use crate::config::table_config::{TableConfig, RowIdStrategy};
use crate::partitioning::{ColumnGroupMapper, overlapping_buckets, i64_to_ordered_u64};
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
    /// Returns (column_name, FilterValue) tuples preserving type information
    pub fn extract_hash_predicates(&self, filters: &[Filter]) -> Vec<(String, FilterValue)> {
        let mut predicates = vec![];

        for dim in &self.config.partitioning.hash_dimensions {
            for filter in filters {
                if filter.column == dim.column && filter.op == FilterOp::Eq {
                    predicates.push((dim.column.clone(), filter.value.clone()));
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

        // If we have bounds, prune row_buckets using order-preserving i64→u64 mapping
        if min_val != i64::MIN || max_val != i64::MAX {
            let chunk_rows = config.partitioning.chunk_rows;

            let min_bucket = if min_val == i64::MIN {
                0
            } else {
                i64_to_ordered_u64(min_val) / chunk_rows
            };

            let max_bucket = if max_val == i64::MAX {
                u64::MAX
            } else {
                i64_to_ordered_u64(max_val) / chunk_rows
            };

            candidates.retain(|chunk| {
                chunk.coord.row_bucket >= min_bucket && chunk.coord.row_bucket <= max_bucket
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

        // Skip range pruning if the range is too large (unbounded)
        let Some(valid_buckets_vec) = overlapping_buckets(effective_min, effective_max, dim.chunk_size) else {
            continue;
        };
        let valid_buckets: HashSet<u64> = valid_buckets_vec.into_iter().collect();

        candidates.retain(|chunk| {
            if chunk.coord.range_buckets.len() > dim_idx {
                valid_buckets.contains(&chunk.coord.range_buckets[dim_idx])
            } else {
                true
            }
        });
    }

    // 4. Hash dimension pruning
    let hash_predicates = extractor.extract_hash_predicates(filters);
    for (i, (_col_name, value)) in hash_predicates.iter().enumerate() {
        if i >= hash_registries.len() {
            continue;
        }

        // Use appropriate lookup method based on value type
        let bucket = match value {
            FilterValue::String(s) => hash_registries[i].lookup_bucket(s)?,
            FilterValue::Int(n) => hash_registries[i].lookup_bucket_numeric(*n)?,
            FilterValue::UInt(n) => hash_registries[i].lookup_bucket_numeric(*n as i64)?,
            FilterValue::Bool(b) => hash_registries[i].lookup_bucket(&b.to_string())?,
        };

        if let Some(bucket) = bucket {
            candidates.retain(|chunk| {
                if chunk.coord.hash_buckets.len() > i {
                    chunk.coord.hash_buckets[i] == bucket
                } else {
                    true
                }
            });
        }
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
    let mut latest_by_coord: HashMap<(u64, u16, Vec<u64>, Vec<u64>), ChunkInfo> = HashMap::new();

    for chunk in candidates {
        let key = (
            chunk.coord.row_bucket,
            chunk.coord.col_group,
            chunk.coord.hash_buckets.clone(),
            chunk.coord.range_buckets.clone(),
        );

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
