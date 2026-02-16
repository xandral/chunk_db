use rayon::prelude::*;
use arrow::record_batch::RecordBatch;
use arrow::array::Array;
use arrow::datatypes::Schema;
use std::path::PathBuf;
use std::sync::Arc;

use crate::query::filter::CompositeFilter;

use crate::config::TableConfig;
use crate::catalog::{VersionCatalog, HashRegistry};
use crate::partitioning::ColumnGroupMapper;
use crate::storage::{ChunkInfo, chunk_path};
use crate::query::filter::{Filter, FilterOp, FilterValue};
use crate::query::chunk_merger::{group_chunks_by_row_key, vertical_join, RowKey};
use crate::query::pruning::prune_chunks;
use crate::Result;
use futures::stream::FuturesUnordered;
use futures::StreamExt;

use tokio::sync::Semaphore;

pub struct DirectExecutor {
    config: Arc<TableConfig>,
    catalog: Arc<VersionCatalog>,
    hash_registries: Arc<Vec<HashRegistry>>,
    column_mapper: Arc<ColumnGroupMapper>,
    base_path: PathBuf,
    schema: Arc<Schema>,
}

impl DirectExecutor {
    pub fn new(
        config: Arc<TableConfig>,
        catalog: Arc<VersionCatalog>,
        hash_registries: Arc<Vec<HashRegistry>>,
        column_mapper: Arc<ColumnGroupMapper>,
        base_path: PathBuf,
    ) -> Self {
        let schema = config.arrow_schema();

        Self {
            config,
            catalog,
            hash_registries,
            column_mapper,
            base_path,
            schema,
        }
    }

    pub async fn execute(
        &self,
        filters: &[Filter],
        composite: Option<CompositeFilter>,
        projection: Option<&[String]>,
    ) -> Result<Vec<RecordBatch>> {
        if let Some(comp) = composite {
            // Complex filter present — use OR logic
            self.execute_composite(comp, projection).await
        } else {
            // Standard AND-only execution
            self.execute_internal(filters, projection, false).await
        }
    }

    pub async fn execute_composite(
        &self,
        composite: CompositeFilter,
        projection: Option<&[String]>,
    ) -> Result<Vec<RecordBatch>> {
        let branches = composite.normalize();

        // For OR operations, we need __row_id for deduplication
        // Ensure it's included in the projection
        let projection_with_row_id = if let Some(cols) = projection {
            let mut extended = cols.to_vec();
            if !extended.contains(&"__row_id".to_string()) {
                extended.insert(0, "__row_id".to_string());
            }
            Some(extended)
        } else {
            projection.map(|p| p.to_vec())
        };

        // Run all branches concurrently
        let futures = branches.iter().map(|filters| {
            let proj_ref = projection_with_row_id.as_deref();
            self.execute_internal(filters, proj_ref, false)
        });
        
        let results = futures::future::try_join_all(futures).await?;

        // Deduplicate based on __row_id
        let deduped = crate::query::chunk_merger::union_and_deduplicate(results)?;

        // If original projection didn't include __row_id, remove it now
        if let Some(original_cols) = projection {
            if !original_cols.contains(&"__row_id".to_string()) {
                // Remove __row_id from each batch
                let final_batches: Vec<RecordBatch> = deduped.into_iter().map(|batch| {
                    let schema = batch.schema();
                    let row_id_idx = schema.index_of("__row_id").ok();
                    
                    if let Some(rid_idx) = row_id_idx {
                        // Keep all columns except __row_id
                        let columns: Vec<_> = batch.columns()
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| *i != rid_idx)
                            .map(|(_, col)| col.clone())
                            .collect();
                        
                        let fields: Vec<_> = schema.fields()
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| *i != rid_idx)
                            .map(|(_, field)| field.clone())
                            .collect();
                        
                        let new_schema = Arc::new(arrow::datatypes::Schema::new(fields));
                        RecordBatch::try_new(new_schema, columns)
                            .map_err(|e| crate::ChunkDbError::Arrow(e))
                    } else {
                        Ok(batch)
                    }
                }).collect::<Result<Vec<_>>>()?;
                
                return Ok(final_batches);
            }
        }

        Ok(deduped)
    }

    /// Internal execute with optimize_for_count flag
    /// When optimize_for_count=true, only reads filter columns if projection=None
    async fn execute_internal(
        &self,
        filters: &[Filter],
        projection: Option<&[String]>,
        optimize_for_count: bool,
    ) -> Result<Vec<RecordBatch>> {
        // 1. Build extended projection that includes filter columns
        let (extended_projection, final_projection) = if let Some(cols) = projection {
            // Collect filter column names
            let filter_cols: std::collections::HashSet<_> =
                filters.iter().map(|f| f.column.as_str()).collect();

            // Create extended projection: user columns + filter columns
            let mut extended = cols.to_vec();
            for filter_col in filter_cols {
                if !extended.contains(&filter_col.to_string()) {
                    extended.push(filter_col.to_string());
                }
            }

            (Some(extended), projection)
            } else if optimize_for_count && !filters.is_empty() {
                // Optimization for COUNT/aggregations: read ONLY filter columns
                // This avoids reading all columns when we just need to count rows
                let filter_cols: Vec<String> = filters
                    .iter()
                    .map(|f| f.column.clone())
                    .collect::<std::collections::HashSet<_>>()
                    .into_iter()
                    .collect();

                (Some(filter_cols), None)
        } else {
            // SELECT * or no filters: read all columns
            (None, None)
        };

        // Build projection indices for pruning
        let projection_indices = if let Some(ref cols) = extended_projection {
            Some(
                cols.iter()
                    .filter_map(|col| {
                        self.schema
                            .fields()
                            .iter()
                            .position(|f| f.name() == col)
                    })
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };

        // 2. Prune chunks
        let pruned_chunks = prune_chunks(
            &self.config,
            &self.catalog,
            &self.hash_registries,
            &self.column_mapper,
            filters,
            projection_indices.as_ref(),
            &self.schema,
        )?;

        if pruned_chunks.is_empty() {
            // Return empty result with proper schema
            let output_schema = if let Some(proj) = final_projection {
                let fields: Vec<_> = proj
                    .iter()
                    .filter_map(|col| {
                        self.schema
                            .fields()
                            .iter()
                            .find(|f| f.name() == col)
                            .cloned()
                    })
                    .collect();
                Arc::new(Schema::new(fields))
            } else {
                self.schema.clone()
            };
            return Ok(vec![RecordBatch::new_empty(output_schema)]);
        }

        // 3. Group by RowKey
        let chunks_by_row_key = group_chunks_by_row_key(pruned_chunks);

        // 4. Flatten to maximize parallelism (avoid nested par_iter)
        // Convert HashMap<RowKey, Vec<ChunkInfo>> to Vec<(RowKey, Vec<ChunkInfo>)>
        let row_key_chunks: Vec<_> = chunks_by_row_key.into_iter().collect();

        // 5. Parallel scan with rayon on ALL row_keys at once
        // PERF: Hardcoded concurrency limit. Should be configurable based on system resources.
        let semaphore = Arc::new(Semaphore::new(128));
        let mut futures = FuturesUnordered::new();

        let shared_ext_projection = Arc::new(extended_projection);
        let shared_filters = Arc::new(filters.to_vec());

        for (row_key, chunks) in row_key_chunks {
            let sem = semaphore.clone();
            let ext_proj = shared_ext_projection.clone(); 
            let flts = shared_filters.clone();     
            
            let r_key = row_key.clone();
            let r_chunks = chunks.clone();

            futures.push(async move {
                let _permit = sem.acquire().await.map_err(|e| {
                    crate::ChunkDbError::InvalidQuery(format!("Semaphore error: {}", e))
                })?;

                self.scan_row_key(
                    &r_key, 
                    &r_chunks, 
                    &flts, 
                    ext_proj.as_ref().as_ref().map(|v| v.as_slice())
                )
            });
        }

        let mut results = Vec::new();
        while let Some(res) = futures.next().await {
            results.push(res?);
        }

        let mut batches: Vec<RecordBatch> = results.into_iter().flatten().collect();

        if let Some(final_proj) = final_projection {
            if shared_ext_projection.is_some() {
                batches = batches
                    .into_par_iter()
                    .map(|batch| apply_final_projection(batch, final_proj))
                    .collect::<Result<Vec<_>>>()?;
            }
        }

        Ok(batches)
    }

    fn scan_row_key(
        &self,
        _row_key: &RowKey,
        chunks: &[ChunkInfo],
        filters: &[Filter],
        projection: Option<&[String]>,
    ) -> Result<Vec<RecordBatch>> {
        // Group chunks by column group
        let mut chunks_by_col_group = std::collections::HashMap::new();
        for chunk in chunks {
            chunks_by_col_group.insert(chunk.coord.col_group, chunk.clone());
        }

        let has_multiple_groups = chunks_by_col_group.len() > 1;

        // Read each column group in parallel
        // Row group pruning is ALWAYS safe (happens within each Parquet file independently)
        // Row-level filtering: defer until after vertical join if multiple groups (to avoid applying filter multiple times)
        // Column group read is independent; vertical_join reconstructs on __row_id
        let apply_filters = !has_multiple_groups;

        let batches: Vec<RecordBatch> = chunks_by_col_group
            .par_iter()
            .map(|(_, chunk)| {
                let path = chunk_path(
                    &self.base_path,
                    &self.config.name,
                    &chunk.coord,
                    chunk.version,
                );  
                read_parquet_sync_with_pruning(path, filters, projection, chunk.coord.col_group, &self.column_mapper, apply_filters)
            }
        )
            .collect::<Result<Vec<_>>>()?;

        if batches.is_empty() {
            return Ok(vec![]);
        }

        // Vertical join if multiple column groups
        let mut result = if batches.len() > 1 {
            vec![vertical_join(batches)?]
        } else {
            batches
        };

        // Apply row-level filters AFTER vertical join (if deferred)
        // Use parallel iterator for better performance with multiple batches
        if !apply_filters && !filters.is_empty() {
            result = result
                .into_par_iter()
                .map(|batch| apply_row_filters(batch, filters))
                .collect::<Result<Vec<_>>>()?;
        }

        Ok(result)
    }


    pub async fn count(&self, filters: &[Filter]) -> Result<i64> {
        // Use optimize_for_count=true to only read filter columns
        let batches = self.execute_internal(filters, None, true).await?;
        // Parallel sum using rayon
        Ok(batches.par_iter().map(|b| b.num_rows() as i64).sum())
    }

    pub async fn sum(&self, filters: &[Filter], column: &str) -> Result<i64> {
        // Use optimize_for_count=true (though with specific projection it doesn't matter)
        let batches = self.execute_internal(filters, Some(&[column.to_string()]), true).await?;

        // Parallel sum across all batches
        let total: i64 = batches
            .par_iter()
            .map(|batch| {
                if batch.num_rows() == 0 {
                    return 0i64;
                }

                let col = batch.column(0);

                // Try different numeric types
                if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                    arr.values().iter().sum::<i64>()
                } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::UInt64Array>() {
                    arr.values().iter().map(|&v| v as i64).sum::<i64>()
                } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int32Array>() {
                    arr.values().iter().map(|&v| v as i64).sum::<i64>()
                } else {
                    0i64
                }
            })
            .sum();

        Ok(total)
    }

    pub async fn avg(&self, filters: &[Filter], column: &str) -> Result<f64> {
        // Single-pass aggregation: compute sum and count together
        let batches = self.execute_internal(filters, Some(&[column.to_string()]), true).await?;

        // Parallel sum and count across all batches
        let (total_sum, total_count): (i64, i64) = batches
            .par_iter()
            .map(|batch| {
                if batch.num_rows() == 0 {
                    return (0i64, 0i64);
                }

                let col = batch.column(0);
                if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                    let sum: i64 = arr.values().iter().sum();
                    let count = arr.len() as i64;
                    (sum, count)
                } else {
                    (0, 0)
                }
            })
            .reduce(|| (0, 0), |(s1, c1), (s2, c2)| (s1 + s2, c1 + c2));

        if total_count == 0 {
            Ok(0.0)
        } else {
            Ok(total_sum as f64 / total_count as f64)
        }
    }

    pub async fn min(&self, filters: &[Filter], column: &str) -> Result<i64> {
        let batches = self.execute_internal(filters, Some(&[column.to_string()]), true).await?;

        // Parallel min across all batches
        let min_val = batches
            .par_iter()
            .filter(|batch| batch.num_rows() > 0)
            .filter_map(|batch| {
                let col = batch.column(0);
                if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                    arr.values().iter().min().copied()
                } else {
                    None
                }
            })
            .min()
            .unwrap_or(i64::MAX);

        Ok(min_val)
    }

    pub async fn max(&self, filters: &[Filter], column: &str) -> Result<i64> {
        let batches = self.execute_internal(filters, Some(&[column.to_string()]), true).await?;

        // Parallel max across all batches
        let max_val = batches
            .par_iter()
            .filter(|batch| batch.num_rows() > 0)
            .filter_map(|batch| {
                let col = batch.column(0);
                if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                    arr.values().iter().max().copied()
                } else {
                    None
                }
            })
            .max()
            .unwrap_or(i64::MIN);

        Ok(max_val)
    }
}

/// Apply final projection to remove filter-only columns
fn apply_final_projection(batch: RecordBatch, projection: &[String]) -> Result<RecordBatch> {
    let schema = batch.schema();
    let mut columns = Vec::new();
    let mut fields = Vec::new();

    for col_name in projection {
        if let Ok(idx) = schema.index_of(col_name) {
            columns.push(batch.column(idx).clone());
            fields.push(schema.field(idx).clone());
        }
    }

    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?)
}

/// Apply row-level filters to a RecordBatch
fn apply_row_filters(batch: RecordBatch, filters: &[Filter]) -> Result<RecordBatch> {
    use arrow::array::{Array, BooleanArray, Int64Array, StringArray, UInt64Array};
    use arrow::compute::{self, kernels::cmp};

    if filters.is_empty() || batch.num_rows() == 0 {
        return Ok(batch);
    }

    // Build a boolean mask for all rows
    let mut mask: Option<BooleanArray> = None;

    for filter in filters {
        // Find column index
        let col_idx = batch.schema().index_of(&filter.column)?;
        let col = batch.column(col_idx);

        // Build filter mask for this column
        let filter_mask = match (&filter.value, filter.op) {
            (FilterValue::Int(val), FilterOp::Eq) => {
                if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    Some(cmp::eq(arr, &Int64Array::new_scalar(*val))?)
                } else {
                    None
                }
            }
            (FilterValue::Int(val), FilterOp::NotEq) => {
                if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    Some(cmp::neq(arr, &Int64Array::new_scalar(*val))?)
                } else {
                    None
                }
            }
            (FilterValue::Int(val), FilterOp::Gt) => {
                if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    Some(cmp::gt(arr, &Int64Array::new_scalar(*val))?)
                } else {
                    None
                }
            }
            (FilterValue::Int(val), FilterOp::GtEq) => {
                if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    Some(cmp::gt_eq(arr, &Int64Array::new_scalar(*val))?)
                } else {
                    None
                }
            }
            (FilterValue::Int(val), FilterOp::Lt) => {
                if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    Some(cmp::lt(arr, &Int64Array::new_scalar(*val))?)
                } else {
                    None
                }
            }
            (FilterValue::Int(val), FilterOp::LtEq) => {
                if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    Some(cmp::lt_eq(arr, &Int64Array::new_scalar(*val))?)
                } else {
                    None
                }
            }
            (FilterValue::UInt(val), op) => {
                if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                    let scalar = UInt64Array::new_scalar(*val);
                    match op {
                        FilterOp::Eq => Some(cmp::eq(arr, &scalar)?),
                        FilterOp::NotEq => Some(cmp::neq(arr, &scalar)?),
                        FilterOp::Gt => Some(cmp::gt(arr, &scalar)?),
                        FilterOp::GtEq => Some(cmp::gt_eq(arr, &scalar)?),
                        FilterOp::Lt => Some(cmp::lt(arr, &scalar)?),
                        FilterOp::LtEq => Some(cmp::lt_eq(arr, &scalar)?),
                    }
                } else {
                    None
                }
            }
            (FilterValue::String(val), FilterOp::Eq) => {
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    Some(cmp::eq(arr, &StringArray::new_scalar(val.as_str()))?)
                } else {
                    None
                }
            }
            (FilterValue::String(val), FilterOp::NotEq) => {
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    Some(cmp::neq(arr, &StringArray::new_scalar(val.as_str()))?)
                } else {
                    None
                }
            }
            (FilterValue::Bool(val), FilterOp::Eq) => {
                if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
                    Some(cmp::eq(arr, &BooleanArray::new_scalar(*val))?)
                } else {
                    None
                }
            }
            // Unsupported filter combinations - return error instead of silently passing all rows
            _ => {
                return Err(crate::ChunkDbError::InvalidQuery(format!(
                    "Unsupported filter: column '{}' with {:?} {:?}",
                    filter.column, filter.op, filter.value
                )));
            }
        };

        // Combine with existing mask (AND operation)
        if let Some(filter_mask) = filter_mask {
            mask = Some(match mask {
                Some(existing) => compute::and(&existing, &filter_mask)?,
                None => filter_mask,
            });
        } else {
            // If downcast failed (column type mismatch), return error
            return Err(crate::ChunkDbError::InvalidQuery(format!(
                "Type mismatch for filter on column '{}'", filter.column
            )));
        }
    }

    // Apply the mask if we built one
    if let Some(mask) = mask {
        let columns: Result<Vec<_>> = batch
            .columns()
            .iter()
            .map(|col| Ok(compute::filter(col, &mask)?))
            .collect();

        Ok(RecordBatch::try_new(batch.schema(), columns?)?)
    } else {
        Ok(batch)
    }
}

/// Synchronous Parquet reading with row group pruning (rayon-compatible)
fn read_parquet_sync_with_pruning(
    path: PathBuf,
    filters: &[Filter],
    projection: Option<&[String]>,
    _col_group: u16,
    _column_mapper: &ColumnGroupMapper,
    apply_row_filter: bool,
) -> Result<RecordBatch> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ProjectionMask;
    use std::fs::File;

    let file = File::open(&path)?;
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    // Row group pruning using statistics
    // IMPORTANT: When using column groups, we CANNOT prune row groups based on filters
    // that reference columns in other groups, because this would cause row_id mismatch
    // during vertical join. Instead, we skip row group pruning and rely on chunk-level
    // pruning, then apply row-level filters after vertical join.
    let metadata = builder.metadata().clone();
    let schema = builder.parquet_schema();

    let mut valid_row_groups = Vec::new();

    // Row group pruning is ALWAYS safe - it operates within a single Parquet file (column group)
    // Row IDs (__row_id) are present in all column group files and correctly aligned by vertical_join
    // Skipping row group pruning for multi-column case was overly conservative
    for i in 0..metadata.num_row_groups() {
        if should_read_row_group(metadata.row_group(i), schema, filters) {
            valid_row_groups.push(i);
        }
    }

    if !valid_row_groups.is_empty() {
        builder = builder.with_row_groups(valid_row_groups);
    }

    // Column projection
    if let Some(cols) = projection {
        let arrow_schema = builder.schema();
        let mut indices = vec![0]; // Always include __row_id at index 0

        for col_name in cols {
            if let Ok(idx) = arrow_schema.index_of(col_name) {
                if !indices.contains(&idx) {
                    indices.push(idx);
                }
            }
        }

        let mask = ProjectionMask::roots(builder.parquet_schema(), indices);
        builder = builder.with_projection(mask);
    }

    // Get schema before consuming builder
    let reader_schema = builder.schema().clone();
    let reader = builder.build()?;

    // Collect and merge batches
    let batches: Vec<_> = reader.collect::<std::result::Result<Vec<_>, _>>()?;

    if batches.is_empty() {
        // Use the actual Parquet schema instead of hardcoded Int64
        return Ok(RecordBatch::new_empty(reader_schema));
    }

    let mut combined = arrow::compute::concat_batches(&batches[0].schema(), &batches)?;

    // Apply row-level filters only if requested
    if apply_row_filter {
        combined = apply_row_filters(combined, filters)?;
    }

    Ok(combined)
}

/// Simple row group pruning using Parquet statistics
fn should_read_row_group(
    row_group: &parquet::file::metadata::RowGroupMetaData,
    schema: &parquet::schema::types::SchemaDescriptor,
    filters: &[Filter],
) -> bool {
    use parquet::file::statistics::Statistics;

    for filter in filters {
        // Find column index
        let col_idx = (0..schema.num_columns())
            .find(|&i| schema.column(i).name() == filter.column);

        let Some(col_idx) = col_idx else {
            continue;
        };

        let Some(stats) = row_group.column(col_idx).statistics() else {
            continue;
        };

        // Check if row group can be pruned based on statistics
        match (&filter.value, &filter.op) {
            (FilterValue::Int(val), op) => {
                if let Statistics::Int64(s) = stats {
                    if let (Some(&min), Some(&max)) = (s.min_opt(), s.max_opt()) {
                        match op {
                            FilterOp::Eq => {
                                if *val < min || *val > max {
                                    return false; // Skip this row group
                                }
                            }
                            FilterOp::Gt => {
                                if max <= *val {
                                    return false;
                                }
                            }
                            FilterOp::GtEq => {
                                if max < *val {
                                    return false;
                                }
                            }
                            FilterOp::Lt => {
                                if min >= *val {
                                    return false;
                                }
                            }
                            FilterOp::LtEq => {
                                if min > *val {
                                    return false;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            (FilterValue::String(val), FilterOp::Eq) => {
                if let Statistics::ByteArray(s) = stats {
                    if let (Some(min), Some(max)) = (s.min_opt(), s.max_opt()) {
                        let min_str = std::str::from_utf8(min.data()).unwrap_or("");
                        let max_str = std::str::from_utf8(max.data()).unwrap_or("");

                        if val.as_str() < min_str || val.as_str() > max_str {
                            return false;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    true // Read this row group
}

