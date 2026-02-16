use arrow::array::{
    Array, ArrayRef, Int64Array, StringArray,
    TimestampMicrosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Schema, Field};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::catalog::{HashRegistry, VersionCatalog};
use crate::config::table_config::TableConfig;
use crate::partitioning::{ColumnGroupMapper, i64_to_ordered_u64, range_bucket, row_bucket};
use crate::storage::{chunk_path, ChunkCoordinate, write_parquet};
use crate::Result;
use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Groups rows by their chunk coordinates
struct ChunkGrouper {
    /// Map from coordinate to row indices
    groups: HashMap<ChunkCoordinate, Vec<usize>>,
}

impl ChunkGrouper {
    fn new() -> Self {
        Self { groups: HashMap::new() }
    }

    fn add_row(&mut self, coord: ChunkCoordinate, row_idx: usize) {
        self.groups.entry(coord).or_default().push(row_idx);
    }
}

/// Batch inserter for a table
pub struct BatchInserter {
    config: TableConfig,
    catalog: Arc<VersionCatalog>,
    hash_registries: Vec<HashRegistry>,
    column_mapper: ColumnGroupMapper,
    base_path: PathBuf,
}

impl BatchInserter {
    pub fn new(
        config: TableConfig,
        catalog: Arc<VersionCatalog>,
        catalog_db: Arc<sled::Db>,
    ) -> Self {
        // Create hash registries for each hash dimension
        let hash_registries: Vec<HashRegistry> = config.partitioning.hash_dimensions.iter()
            .map(|dim| {
                HashRegistry::new(
                    catalog_db.clone(),
                    &dim.column,
                    dim.num_buckets,
                    dim.strategy.clone(),
                )
            })
            .collect();

        let column_mapper = ColumnGroupMapper::new(&config);
        let base_path = PathBuf::from(&config.storage.base_path);

        Self {
            config,
            catalog,
            hash_registries,
            column_mapper,
            base_path,
        }
    }

    /// Insert a batch of data
    ///
    /// The batch should include a `__row_id` column (UInt64) for row identification.
    /// If not present, sequential IDs starting from 0 will be assigned.
    pub fn insert(&self, batch: &RecordBatch) -> Result<u64> {
        let version = self.catalog.next_transaction_id()?;

        // 1. Calculate chunk coordinates for each row
        let num_rows = batch.num_rows();
        let mut grouper = ChunkGrouper::new();

        // Get or generate row IDs
        let row_ids = self.get_or_generate_row_ids(batch, num_rows)?;

        // Calculate hash buckets for each row
        let hash_buckets_per_row = self.calculate_hash_buckets(batch)?;

        // Calculate range buckets for each row
        let range_buckets_per_row = self.calculate_range_buckets(batch)?;

        // Update range dimension statistics
        self.update_range_stats(batch)?;

        // Group rows by coordinate (for each column group)
        for row_idx in 0..num_rows {
            let row_id = row_ids[row_idx];
            let row_bucket_idx = row_bucket(row_id, self.config.partitioning.chunk_rows);

            let hash_buckets: Vec<u64> = hash_buckets_per_row.iter()
                .map(|dim_buckets| dim_buckets[row_idx])
                .collect();

            let range_buckets: Vec<u64> = range_buckets_per_row.iter()
                .map(|dim_buckets| dim_buckets[row_idx])
                .collect();

            // Create coordinate for each column group
            for col_group in 0..self.column_mapper.num_groups() {
                let coord = ChunkCoordinate::new(
                    row_bucket_idx,
                    col_group,
                    hash_buckets.clone(),
                    range_buckets.clone(),
                );
                grouper.add_row(coord, row_idx);
            }
        }

        // 2. Write chunks (with merge-on-write to prevent row loss)
        for (coord, row_indices) in grouper.groups {
            // Get columns for this column group
            let group_columns = self.column_mapper.get_columns_in_group(coord.col_group);

            // Build schema for this column group (include __row_id for merging)
            let mut fields: Vec<Field> = vec![
                Field::new("__row_id", DataType::UInt64, false),
            ];

            let mut arrays: Vec<ArrayRef> = vec![
                Arc::new(self.take_indices(&row_ids, &row_indices)) as ArrayRef,
            ];

            let batch_schema = batch.schema();
            for col_name in group_columns {
                let col_idx = batch_schema.index_of(col_name)?;
                let col = batch.column(col_idx);
                let field = batch_schema.field(col_idx);

                fields.push(Field::clone(field));
                arrays.push(self.take_array(col, &row_indices)?);
            }

            let chunk_schema = Arc::new(Schema::new(fields));
            let chunk_batch = RecordBatch::try_new(chunk_schema, arrays)?;

            // Merge with existing data if this coordinate already has a chunk
            let final_batch = if let Some(prev_version) = self.catalog.get_latest_version(&self.config.name, &coord)? {
                let existing_path = chunk_path(&self.base_path, &self.config.name, &coord, prev_version);
                if existing_path.exists() {
                    let existing_batch = Self::read_parquet_file(&existing_path)?;
                    // Concat then deduplicate by __row_id (keep latest)
                    let merged = arrow::compute::concat_batches(&existing_batch.schema(), &[existing_batch, chunk_batch])?;
                    Self::deduplicate_by_row_id(merged)?
                } else {
                    chunk_batch
                }
            } else {
                chunk_batch
            };

            // Write to file
            let path = chunk_path(&self.base_path, &self.config.name, &coord, version);
            write_parquet(&path, &final_batch, None)?;

            // Update catalog
            self.catalog.update_version(&self.config.name, &coord, version)?;
        }

        self.catalog.flush()?;

        Ok(version)
    }

    fn get_or_generate_row_ids(&self, batch: &RecordBatch, num_rows: usize) -> Result<Vec<u64>> {
        use crate::config::table_config::RowIdStrategy;
        use xxhash_rust::xxh3::xxh3_64;

        // Check if user provided __row_id explicitly
        if let Ok(idx) = batch.schema().index_of("__row_id") {
            let col = batch.column(idx);
            let arr = col.as_any().downcast_ref::<UInt64Array>()
                .ok_or_else(|| crate::ChunkDbError::Config(
                    "__row_id must be UInt64".to_string()
                ))?;
            return Ok(arr.values().to_vec());
        }

        // Generate based on strategy
        match &self.config.row_id_strategy {
            RowIdStrategy::Snowflake => {
                // Allocate row IDs atomically from catalog
                let start_id = self.catalog.allocate_row_ids(num_rows as u64)?;
                Ok((start_id..start_id + num_rows as u64).collect())
            }

            RowIdStrategy::SingleColumn(col_name) => {
                // Use single column as row_id (numeric or string types)
                let col_idx = batch.schema().index_of(col_name)?;
                let col = batch.column(col_idx);

                // Try different types
                if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                    Ok(arr.values().to_vec())
                } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    // Order-preserving mapping: negative values → lower buckets
                    Ok(arr.values().iter().map(|&v| i64_to_ordered_u64(v)).collect())
                } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::UInt32Array>() {
                    Ok(arr.values().iter().map(|&v| v as u64).collect())
                } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int32Array>() {
                    Ok(arr.values().iter().map(|&v| i64_to_ordered_u64(v as i64)).collect())
                } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    // Hash string values to u64 (for UUIDs, etc.)
                    Ok(arr.iter()
                        .map(|opt_str| {
                            let s = opt_str.expect("Primary key column contains null value");
                            xxh3_64(s.as_bytes())
                        })
                        .collect())
                } else {
                    Err(crate::ChunkDbError::Config(
                        format!("Primary key column '{}' must be Int64, UInt64, Int32, UInt32, or Utf8, got {:?}",
                                col_name, col.data_type())
                    ))
                }
            }

            RowIdStrategy::CompositeHash(col_names) => {
                // Hash multiple columns together using length-prefix encoding
                // to avoid collisions like ["ab", "c"] vs ["a", "bc"]
                let mut row_ids = Vec::with_capacity(num_rows);

                for row_idx in 0..num_rows {
                    let mut hash_input = Vec::new();

                    for col_name in col_names {
                        let col_idx = batch.schema().index_of(col_name)?;
                        let col = batch.column(col_idx);

                        // Serialize column value with length prefix to prevent collisions
                        match col.data_type() {
                            DataType::Utf8 => {
                                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                                let bytes = arr.value(row_idx).as_bytes();
                                // Length prefix (4 bytes) + data
                                hash_input.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                                hash_input.extend_from_slice(bytes);
                            }
                            DataType::Int64 => {
                                let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                                // Fixed-size types: length prefix not strictly needed but consistent
                                hash_input.extend_from_slice(&8u32.to_le_bytes());
                                hash_input.extend_from_slice(&arr.value(row_idx).to_le_bytes());
                            }
                            DataType::UInt64 => {
                                let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                                hash_input.extend_from_slice(&8u32.to_le_bytes());
                                hash_input.extend_from_slice(&arr.value(row_idx).to_le_bytes());
                            }
                            DataType::Int32 => {
                                let arr = col.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
                                hash_input.extend_from_slice(&4u32.to_le_bytes());
                                hash_input.extend_from_slice(&arr.value(row_idx).to_le_bytes());
                            }
                            DataType::UInt32 => {
                                let arr = col.as_any().downcast_ref::<arrow::array::UInt32Array>().unwrap();
                                hash_input.extend_from_slice(&4u32.to_le_bytes());
                                hash_input.extend_from_slice(&arr.value(row_idx).to_le_bytes());
                            }
                            _ => {
                                return Err(crate::ChunkDbError::Config(
                                    format!("Unsupported type for composite hash: {:?}", col.data_type())
                                ));
                            }
                        }
                    }

                    // Generate hash
                    let row_id = xxh3_64(&hash_input);
                    row_ids.push(row_id);
                }

                Ok(row_ids)
            }
        }
    }

    fn calculate_hash_buckets(&self, batch: &RecordBatch) -> Result<Vec<Vec<u64>>> {
        let num_rows = batch.num_rows();
        let mut result = vec![];

        for (i, dim) in self.config.partitioning.hash_dimensions.iter().enumerate() {
            let col_idx = batch.schema().index_of(&dim.column)?;
            let col = batch.column(col_idx);

            let buckets: Vec<u64> = match col.data_type() {
                DataType::Utf8 => {
                    let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                    (0..num_rows)
                        .map(|row_idx| {
                            let val = arr.value(row_idx);
                            self.hash_registries[i].get_bucket(val).unwrap_or(0)
                        })
                        .collect()
                }
                DataType::Int64 => {
                    let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                    (0..num_rows)
                        .map(|row_idx| {
                            let val = arr.value(row_idx);
                            self.hash_registries[i].get_bucket_numeric(val).unwrap_or(0)
                        })
                        .collect()
                }
                // BUG: Unsupported hash column types silently assigned to bucket 0,
                // destroying hash pruning effectiveness. Should add UInt64 support
                // and error on truly unsupported types at table creation time.
                _ => vec![0; num_rows],
            };

            result.push(buckets);
        }

        Ok(result)
    }

    fn calculate_range_buckets(&self, batch: &RecordBatch) -> Result<Vec<Vec<u64>>> {
        let num_rows = batch.num_rows();
        let mut result = vec![];

        for dim in &self.config.partitioning.range_dimensions {
            let col_idx = batch.schema().index_of(&dim.column)?;
            let col = batch.column(col_idx);

            let buckets: Vec<u64> = match col.data_type() {
                DataType::Int64 => {
                    let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                    (0..num_rows)
                        .map(|i| range_bucket(arr.value(i), dim.chunk_size))
                        .collect()
                }
                DataType::Timestamp(_, _) => {
                    let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                    (0..num_rows)
                        .map(|i| {
                            // Convert microseconds to the chunk_size unit (assuming seconds)
                            let val = arr.value(i) / 1_000_000; // to seconds
                            range_bucket(val, dim.chunk_size)
                        })
                        .collect()
                }
                _ => vec![0; num_rows],
            };

            result.push(buckets);
        }

        Ok(result)
    }

    /// Read an existing parquet chunk file for merge-on-write
    fn read_parquet_file(path: &PathBuf) -> Result<RecordBatch> {
        let file = std::fs::File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;
        let batches: Vec<RecordBatch> = reader
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if batches.is_empty() {
            return Err(crate::ChunkDbError::Config(
                "Empty parquet file during merge".to_string(),
            ));
        }
        Ok(arrow::compute::concat_batches(&batches[0].schema(), &batches)?)
    }

    /// Deduplicate rows by __row_id, keeping only the last occurrence of each row_id
    /// This implements upsert semantics for merge-on-write
    fn deduplicate_by_row_id(batch: RecordBatch) -> Result<RecordBatch> {
        use std::collections::HashMap;
        use arrow::array::BooleanArray;

        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(batch);
        }

        // Extract __row_id column (always first column)
        let row_id_col = batch.column(0);
        let row_ids = row_id_col.as_any().downcast_ref::<UInt64Array>()
            .ok_or_else(|| crate::ChunkDbError::Config(
                "__row_id must be UInt64".to_string()
            ))?;

        // Track last occurrence index for each row_id
        let mut last_occurrence: HashMap<u64, usize> = HashMap::new();
        for (idx, &row_id) in row_ids.values().iter().enumerate() {
            last_occurrence.insert(row_id, idx);
        }

        // Build filter mask: keep row if it's the last occurrence of its row_id
        let keep_mask: Vec<bool> = (0..num_rows)
            .map(|idx| {
                let row_id = row_ids.value(idx);
                last_occurrence.get(&row_id) == Some(&idx)
            })
            .collect();

        let filter = BooleanArray::from(keep_mask);

        // Filter all columns
        let filtered_columns: Result<Vec<ArrayRef>> = batch.columns()
            .iter()
            .map(|col| Ok(arrow::compute::filter(col.as_ref(), &filter)?))
            .collect();

        Ok(RecordBatch::try_new(batch.schema(), filtered_columns?)?)
    }

    fn take_indices(&self, values: &[u64], indices: &[usize]) -> UInt64Array {
        UInt64Array::from_iter_values(indices.iter().map(|&i| values[i]))
    }

    fn take_array(&self, array: &ArrayRef, indices: &[usize]) -> Result<ArrayRef> {
        let indices_arr = UInt64Array::from_iter_values(indices.iter().map(|&i| i as u64));

        // Use arrow compute take
        Ok(arrow::compute::take(array.as_ref(), &indices_arr, None)?)
    }

    fn update_range_stats(&self, batch: &RecordBatch) -> Result<()> {
        use crate::catalog::RangeDimensionStats;

        for dim in &self.config.partitioning.range_dimensions {
            let col_idx = batch.schema().index_of(&dim.column)?;
            let col = batch.column(col_idx);

            let (batch_min, batch_max) = self.compute_column_min_max(col)?;

            // Get or create stats
            let mut stats = self.catalog
                .get_range_stats(&self.config.name, &dim.column)?
                .unwrap_or_else(|| RangeDimensionStats::new(&dim.column));

            // Update with batch values
            stats.update(batch_min, batch_max, batch.num_rows() as u64);

            // Persist
            self.catalog.update_range_stats(&self.config.name, &dim.column, &stats)?;
        }

        Ok(())
    }

    fn compute_column_min_max(&self, col: &ArrayRef) -> Result<(i64, i64)> {
        use arrow::datatypes::DataType;

        match col.data_type() {
            DataType::Int64 => {
                let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                let min = arrow::compute::min(arr).unwrap_or(i64::MAX);
                let max = arrow::compute::max(arr).unwrap_or(i64::MIN);
                Ok((min, max))
            }
            DataType::Timestamp(_, _) => {
                let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                // Convert to seconds for consistency
                let min = arr.iter().flatten().min().map(|v| v / 1_000_000).unwrap_or(i64::MAX);
                let max = arr.iter().flatten().max().map(|v| v / 1_000_000).unwrap_or(i64::MIN);
                Ok((min, max))
            }
            _ => {
                // Unsupported type for range stats
                Ok((i64::MAX, i64::MIN))
            }
        }
    }
}


