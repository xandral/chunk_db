use arrow::array::{RecordBatch, UInt64Array, ArrayRef, BooleanArray};
use arrow::compute::{take, filter};
use arrow::datatypes::{Schema, Field};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::Result;
use crate::storage::ChunkCoordinate;
use crate::storage::ChunkInfo;

/// Key identifying a row group (same rows, different columns)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowKey {
    pub row_bucket: u64,
    pub hash_buckets: Vec<u64>,
    pub range_buckets: Vec<u64>,
}

impl From<&ChunkCoordinate> for RowKey {
    fn from(coord: &ChunkCoordinate) -> Self {
        Self {
            row_bucket: coord.row_bucket,
            hash_buckets: coord.hash_buckets.clone(),
            range_buckets: coord.range_buckets.clone(),
        }
    }
}

/// Group chunks by RowKey
pub fn group_chunks_by_row_key(
    chunks: Vec<ChunkInfo>,
) -> HashMap<RowKey, Vec<ChunkInfo>> {
    let mut groups: HashMap<RowKey, Vec<_>> = HashMap::new();

    for chunk in chunks {
        let key = RowKey::from(&chunk.coord);
        groups.entry(key).or_default().push(chunk);
    }

    groups
}

/// Extract row_ids from a batch as a HashSet for O(1) intersection
fn extract_row_id_set(batch: &RecordBatch) -> Result<HashSet<u64>> {
    let row_id_idx = batch.schema()
        .index_of("__row_id")
        .map_err(|_| crate::ChunkDbError::Config("__row_id column required".into()))?;

    let row_ids = batch.column(row_id_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| crate::ChunkDbError::Config("__row_id must be UInt64".into()))?;

    Ok(row_ids.values().iter().copied().collect())
}

/// Build a HashMap row_id -> index for O(1) lookup
fn build_row_id_index(batch: &RecordBatch) -> Result<HashMap<u64, usize>> {
    let row_id_idx = batch.schema()
        .index_of("__row_id")
        .map_err(|_| crate::ChunkDbError::Config("__row_id column required".into()))?;

    let row_ids = batch.column(row_id_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| crate::ChunkDbError::Config("__row_id must be UInt64".into()))?;

    Ok(row_ids.values()
        .iter()
        .enumerate()
        .map(|(idx, &id)| (id, idx))
        .collect())
}

/// Filter a batch to keep only rows with row_id in the common set
fn filter_batch_by_row_ids(batch: &RecordBatch, common_ids: &HashSet<u64>) -> Result<RecordBatch> {
    let row_id_idx = batch.schema().index_of("__row_id").unwrap();
    let row_ids = batch.column(row_id_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    // Build boolean mask
    let mask: BooleanArray = row_ids.values()
        .iter()
        .map(|id| Some(common_ids.contains(id)))
        .collect();

    // Apply filter to all columns
    let filtered_columns: Vec<ArrayRef> = batch.columns()
        .iter()
        .map(|col| filter(col.as_ref(), &mask).map_err(|e| crate::ChunkDbError::Arrow(e)))
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(batch.schema(), filtered_columns).map_err(Into::into)
}

// In crate::query::chunk_merger

/// Merges multiple sets of batches (from OR branches) and removes duplicates based on __row_id.
pub fn union_and_deduplicate(
    batch_sets: Vec<Vec<RecordBatch>>
) -> Result<Vec<RecordBatch>> {
    if batch_sets.is_empty() {
        return Ok(vec![]);
    }

    // Flatten into a single stream of batches
    let all_batches: Vec<RecordBatch> = batch_sets.into_iter().flatten().collect();
    
    if all_batches.is_empty() {
        return Ok(vec![]);
    }

    let schema = all_batches[0].schema();
    let row_id_idx = schema.index_of("__row_id")
        .map_err(|_| crate::ChunkDbError::Config("__row_id required for OR operations".into()))?;

    // Global set of seen row IDs to prevent duplicates across branches
    let mut seen_ids = HashSet::new();
    let mut deduped_batches = Vec::new();

    for batch in all_batches {
        let row_ids = batch.column(row_id_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| crate::ChunkDbError::Arrow(arrow::error::ArrowError::CastError("Row ID not u64".into())))?;

        // 1. Check if we need to filter this batch
        let mut keep_bitmap = Vec::with_capacity(batch.num_rows());
        let mut has_duplicates = false;

        for &id in row_ids.values() {
            if seen_ids.insert(id) {
                keep_bitmap.push(true);
            } else {
                keep_bitmap.push(false);
                has_duplicates = true;
            }
        }

        // 2. If no duplicates, keep batch as is. If duplicates, filter.
        if !has_duplicates {
            deduped_batches.push(batch);
        } else if keep_bitmap.iter().any(|&b| b) {
            // Some new rows found, filter the batch
            let mask = BooleanArray::from(keep_bitmap);
            let filtered_cols = batch.columns().iter()
                .map(|col| filter(col.as_ref(), &mask).map_err(crate::ChunkDbError::Arrow))
                .collect::<Result<Vec<_>>>()?;
            
            deduped_batches.push(RecordBatch::try_new(schema.clone(), filtered_cols)?);
        }
    }

    Ok(deduped_batches)
}

/// Vertical join of batches on the same RowKey (different column groups)
///
/// Implements inner join semantics: only rows present in ALL batches
/// are included in the result. Uses HashMap for O(1) lookup.
pub fn vertical_join(batches: Vec<RecordBatch>) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Err(crate::ChunkDbError::Config("No batches to join".into()));
    }

    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
    }

    // 1. Compute row_id intersection (inner join semantics)
    let mut common_ids: HashSet<u64> = extract_row_id_set(&batches[0])?;

    for batch in batches.iter().skip(1) {
        let batch_ids = extract_row_id_set(batch)?;
        common_ids = common_ids.intersection(&batch_ids).copied().collect();
    }

    // No common rows — return empty batch
    if common_ids.is_empty() {
        let mut all_fields: Vec<Field> = vec![];
        let mut seen: HashSet<String> = HashSet::new();
        for batch in &batches {
            for field in batch.schema().fields() {
                if !seen.contains(field.name()) {
                    seen.insert(field.name().clone());
                    all_fields.push(field.as_ref().clone());
                }
            }
        }
        let schema = Arc::new(Schema::new(all_fields));
        return Ok(RecordBatch::new_empty(schema));
    }

    // 2. Filter each batch to keep only common rows
    let filtered_batches: Vec<RecordBatch> = batches.iter()
        .map(|batch| filter_batch_by_row_ids(batch, &common_ids))
        .collect::<Result<Vec<_>>>()?;

    // 3. Use the first filtered batch as reference for row ordering
    let reference_batch = &filtered_batches[0];
    let ref_row_id_idx = reference_batch.schema().index_of("__row_id").unwrap();
    let ref_row_ids = reference_batch.column(ref_row_id_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    // 4. Build final columns
    let mut final_fields: Vec<Field> = vec![];
    let mut final_columns: Vec<ArrayRef> = vec![];
    let mut seen_columns: HashSet<String> = HashSet::new();

    for (batch_idx, batch) in filtered_batches.iter().enumerate() {
        // Build HashMap for O(1) lookup instead of O(n)
        let batch_id_to_idx = build_row_id_index(batch)?;

        // Compute indices to reorder this batch to match the reference ordering
        let take_indices: Option<UInt64Array> = if batch_idx == 0 {
            None // First batch defines the ordering
        } else {
            let indices: Vec<u64> = ref_row_ids.values().iter()
                .map(|&ref_id| {
                    // O(1) lookup instead of O(n) scan
                    *batch_id_to_idx.get(&ref_id).unwrap() as u64
                })
                .collect();

            Some(UInt64Array::from(indices))
        };

        // Add columns from this batch
        for (col_idx, field) in batch.schema().fields().iter().enumerate() {
            let col_name = field.name();

            // Skip columns already added (e.g., __row_id)
            if seen_columns.contains(col_name) {
                continue;
            }
            seen_columns.insert(col_name.clone());

            let column = batch.column(col_idx);

            let final_column = if let Some(ref indices) = take_indices {
                take(column.as_ref(), indices, None)?
            } else {
                column.clone()
            };

            final_fields.push(field.as_ref().clone());
            final_columns.push(final_column);
        }
    }

    let final_schema = Arc::new(Schema::new(final_fields));
    RecordBatch::try_new(final_schema, final_columns).map_err(Into::into)
}

/// Remove the __row_id column and apply projection
pub fn apply_projection(
    batch: RecordBatch,
    projection: Option<&Vec<usize>>,
    output_schema: &Schema,
) -> Result<RecordBatch> {
    use arrow::datatypes::Field;

    let row_id_idx = batch.schema().index_of("__row_id").ok();

    // Build columns and fields in order
    let mut final_columns: Vec<ArrayRef> = vec![];
    let mut final_fields: Vec<Field> = vec![];

    if let Some(proj) = projection {
        // Use the specified projection
        for &out_idx in proj {
            let col_name = output_schema.field(out_idx).name();
            if let Ok(batch_idx) = batch.schema().index_of(col_name) {
                final_columns.push(batch.column(batch_idx).clone());
                // Use field from the batch instead of from output_schema
                final_fields.push(Field::clone(batch.schema().field(batch_idx)));
            }
        }
    } else {
        // All columns except __row_id
        for (i, field) in batch.schema().fields().iter().enumerate() {
            if Some(i) != row_id_idx {
                final_columns.push(batch.column(i).clone());
                final_fields.push(Field::clone(field));
            }
        }
    }

    let final_schema = Arc::new(Schema::new(final_fields));
    RecordBatch::try_new(final_schema, final_columns).map_err(Into::into)
}


