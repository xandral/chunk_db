use std::collections::HashSet;
use arrow::array::{UInt64Array, BooleanArray};
use arrow::compute;
use arrow::record_batch::RecordBatch;

use super::patch_log::{PatchEntry, PatchOp};

pub fn apply_patches(mut batch: RecordBatch, patches: &[PatchEntry]) -> crate::Result<RecordBatch> {
    for patch in patches {
        match &patch.op {
            PatchOp::Insert(new_rows) => batch = append_rows(batch, new_rows)?,
            PatchOp::Update(update_batch) => batch = merge_update(batch, update_batch)?,
            PatchOp::Delete(row_ids) => batch = filter_out_rows(batch, row_ids)?,
        }
    }
    Ok(batch)
}

fn merge_update(base: RecordBatch, update: &RecordBatch) -> crate::Result<RecordBatch> {
    let base_ids = base.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
    let update_ids = update.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();

    let base_id_set: HashSet<u64> = (0..base_ids.len()).map(|i| base_ids.value(i)).collect();
    let update_id_set: HashSet<u64> = (0..update_ids.len())
        .map(|i| update_ids.value(i))
        .filter(|id| base_id_set.contains(id))
        .collect();

    if update_id_set.is_empty() {
        return Ok(base);
    }

    // Remove rows from base that will be replaced
    let keep_mask: BooleanArray = (0..base_ids.len())
        .map(|i| Some(!update_id_set.contains(&base_ids.value(i))))
        .collect();
    let filtered_base = filter_batch(&base, &keep_mask)?;

    // Keep only update rows that exist in base
    let update_mask: BooleanArray = (0..update_ids.len())
        .map(|i| Some(update_id_set.contains(&update_ids.value(i))))
        .collect();
    let filtered_update = filter_batch(update, &update_mask)?;

    Ok(compute::concat_batches(&base.schema(), &[filtered_base, filtered_update])?)
}

fn filter_out_rows(batch: RecordBatch, row_ids: &[u64]) -> crate::Result<RecordBatch> {
    let id_set: HashSet<u64> = row_ids.iter().copied().collect();
    let base_ids = batch.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
    let mask: BooleanArray = (0..base_ids.len())
        .map(|i| Some(!id_set.contains(&base_ids.value(i))))
        .collect();
    filter_batch(&batch, &mask)
}

fn append_rows(base: RecordBatch, new_rows: &RecordBatch) -> crate::Result<RecordBatch> {
    Ok(compute::concat_batches(&base.schema(), &[base, new_rows.clone()])?)
}

fn filter_batch(batch: &RecordBatch, mask: &BooleanArray) -> crate::Result<RecordBatch> {
    let filtered_columns: crate::Result<Vec<_>> = batch.columns().iter()
        .map(|col| Ok(compute::filter(col, mask)?))
        .collect();
    Ok(RecordBatch::try_new(batch.schema(), filtered_columns?)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::patch_log::{PatchEntry, PatchOp};
    use arrow::array::{Int64Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(row_ids: Vec<u64>, values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("__row_id", DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(UInt64Array::from(row_ids)),
            Arc::new(Int64Array::from(values)),
        ]).unwrap()
    }

    fn assert_row_value(batch: &RecordBatch, row_id: u64, expected_value: i64) {
        let ids = batch.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
        let vals = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..batch.num_rows() {
            if ids.value(i) == row_id {
                assert_eq!(vals.value(i), expected_value,
                    "row_id={}: expected {}, got {}", row_id, expected_value, vals.value(i));
                return;
            }
        }
        panic!("row_id={} not found in batch", row_id);
    }

    #[test]
    fn test_apply_no_patches() {
        let base = make_batch(vec![1, 2, 3], vec![10, 20, 30]);
        let result = apply_patches(base, &[]).unwrap();
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn test_merge_update_single() {
        let base = make_batch(vec![1, 2, 3], vec![10, 20, 30]);
        let update = make_batch(vec![2], vec![999]);
        let result = apply_patches(base, &[PatchEntry { tx_id: 1, op: PatchOp::Update(update) }]).unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_row_value(&result, 2, 999);
        assert_row_value(&result, 1, 10);
        assert_row_value(&result, 3, 30);
    }

    #[test]
    fn test_merge_update_missing_row() {
        let base = make_batch(vec![1, 2], vec![10, 20]);
        let update = make_batch(vec![99], vec![999]);
        let result = apply_patches(base, &[PatchEntry { tx_id: 1, op: PatchOp::Update(update) }]).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_row_value(&result, 1, 10);
        assert_row_value(&result, 2, 20);
    }

    #[test]
    fn test_filter_out() {
        let batch = make_batch(vec![1, 2, 3, 4, 5], vec![10, 20, 30, 40, 50]);
        let result = apply_patches(batch, &[PatchEntry { tx_id: 1, op: PatchOp::Delete(vec![2, 4]) }]).unwrap();
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn test_filter_out_missing() {
        let batch = make_batch(vec![1, 2], vec![10, 20]);
        let result = apply_patches(batch, &[PatchEntry { tx_id: 1, op: PatchOp::Delete(vec![99]) }]).unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn test_append_rows() {
        let base = make_batch(vec![1, 2], vec![10, 20]);
        let new = make_batch(vec![3, 4], vec![30, 40]);
        let result = apply_patches(base, &[PatchEntry { tx_id: 1, op: PatchOp::Insert(new) }]).unwrap();
        assert_eq!(result.num_rows(), 4);
    }

    #[test]
    fn test_apply_mixed_sequence() {
        let base = make_batch(vec![1, 2, 3], vec![10, 20, 30]);
        let patches = vec![
            PatchEntry { tx_id: 1, op: PatchOp::Update(make_batch(vec![2], vec![200])) },
            PatchEntry { tx_id: 2, op: PatchOp::Delete(vec![3]) },
            PatchEntry { tx_id: 3, op: PatchOp::Insert(make_batch(vec![4], vec![40])) },
        ];
        let result = apply_patches(base, &patches).unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_row_value(&result, 1, 10);
        assert_row_value(&result, 2, 200);
        assert_row_value(&result, 4, 40);
    }

    #[test]
    fn test_update_then_delete_same_row() {
        let base = make_batch(vec![1, 2], vec![10, 20]);
        let patches = vec![
            PatchEntry { tx_id: 1, op: PatchOp::Update(make_batch(vec![1], vec![999])) },
            PatchEntry { tx_id: 2, op: PatchOp::Delete(vec![1]) },
        ];
        let result = apply_patches(base, &patches).unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_row_value(&result, 2, 20);
    }
}
