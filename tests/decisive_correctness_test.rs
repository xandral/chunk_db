//! Regression tests for correctness risks that directly affect the viability
//! of rectangular, multidimensional chunks as an HTAP storage primitive.

use std::sync::{Arc, Barrier};

use arrow::array::{Int64Array, StringArray, UInt64Array};
use chunk_db::{ChunkDb, Filter, RecordBatch, TableBuilder};

fn int64_row_id(value: i64) -> u64 {
    chunk_db::partitioning::i64_to_ordered_u64(value)
}

#[tokio::test]
async fn filter_on_second_hash_dimension_uses_its_own_bucket_axis() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    let mut db = ChunkDb::open(path).unwrap();

    let config = TableBuilder::new("multi_hash", path)
        .add_column("id", "Int64", false)
        .add_column("region", "Utf8", false)
        .add_column("sensor", "Utf8", false)
        .add_column("value", "Int64", false)
        .chunk_rows(u64::MAX)
        .add_hash_dimension("region", 16)
        .add_hash_dimension("sensor", 16)
        .with_primary_key_as_row_id("id")
        .build();
    db.create_table(config).unwrap();

    let rows = 512i64;
    let sensors: Vec<String> = (0..rows)
        .map(|i| format!("sensor-{}", i % 16))
        .collect();
    let regions: Vec<String> = (0..rows)
        .map(|i| format!("region-{}", (i * 7 + 3) % 11))
        .collect();
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int64Array::from_iter_values(0..rows)) as _),
        ("region", Arc::new(StringArray::from(regions)) as _),
        ("sensor", Arc::new(StringArray::from(sensors)) as _),
        ("value", Arc::new(Int64Array::from_iter_values(0..rows)) as _),
    ])
    .unwrap();
    db.insert("multi_hash", &batch).unwrap();

    // This predicate is intentionally only on dimension index 1. The former
    // implementation enumerated the extracted predicates and accidentally
    // compared it with hash coordinate 0.
    let count = db
        .select_all("multi_hash")
        .filter(Filter::eq("sensor", "sensor-9"))
        .count()
        .await
        .unwrap();
    assert_eq!(count, 32);
}

#[tokio::test]
async fn an_older_snapshot_never_consumes_a_future_cache_entry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    let mut db = ChunkDb::open(path).unwrap();

    let config = TableBuilder::new("snapshot_cache", path)
        .add_column("id", "Int64", false)
        .add_column("value", "Int64", false)
        .chunk_rows(u64::MAX)
        .with_primary_key_as_row_id("id")
        .build();
    db.create_table(config).unwrap();

    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int64Array::from_iter_values(0..10)) as _),
        (
            "value",
            Arc::new(Int64Array::from_iter_values((0..10).map(|i| i * 10))) as _,
        ),
    ])
    .unwrap();
    db.insert("snapshot_cache", &batch).unwrap();

    let before_delete = db.create_executor("snapshot_cache").unwrap();
    db.delete_rows("snapshot_cache", &[int64_row_id(4)])
        .unwrap();

    // Populate the shared cache at the newer snapshot first. This ordering is
    // what the pre-regression snapshot test did not exercise.
    assert_eq!(db.select_all("snapshot_cache").count().await.unwrap(), 9);

    let old_batches = before_delete.execute(&[], None, None).await.unwrap();
    let old_count: usize = old_batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(old_count, 10, "future cache state leaked into an old snapshot");
}

#[tokio::test]
async fn projected_column_group_query_applies_full_schema_update_patch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    let mut db = ChunkDb::open(path).unwrap();

    let config = TableBuilder::new("projected_patch", path)
        .add_column("id", "Int64", false)
        .add_column("a", "Int64", false)
        .add_column("b", "Int64", false)
        .chunk_rows(u64::MAX)
        .add_column_group(vec!["id", "a"])
        .add_column_group(vec!["b"])
        .with_primary_key_as_row_id("id")
        .build();
    db.create_table(config).unwrap();

    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int64Array::from_iter_values(0..20)) as _),
        (
            "a",
            Arc::new(Int64Array::from_iter_values((0..20).map(|i| i * 10))) as _,
        ),
        (
            "b",
            Arc::new(Int64Array::from_iter_values((0..20).map(|i| i * 100))) as _,
        ),
    ])
    .unwrap();
    db.insert("projected_patch", &batch).unwrap();

    let update = RecordBatch::try_from_iter(vec![
        (
            "__row_id",
            Arc::new(UInt64Array::from(vec![int64_row_id(7)])) as _,
        ),
        ("id", Arc::new(Int64Array::from(vec![7])) as _),
        ("a", Arc::new(Int64Array::from(vec![777])) as _),
        ("b", Arc::new(Int64Array::from(vec![7_777])) as _),
    ])
    .unwrap();
    db.update_rows("projected_patch", &update).unwrap();

    // Only group 0 is requested, while the update patch carries both groups.
    let batches = db
        .select(&["a"])
        .from("projected_patch")
        .filter(Filter::eq("id", 7))
        .execute()
        .await
        .unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 1);
    let batch = batches.iter().find(|batch| batch.num_rows() == 1).unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.value(0), 777);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_merge_on_write_inserts_preserve_every_batch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let mut db = ChunkDb::open(&path).unwrap();

    let config = TableBuilder::new("concurrent", &path)
        .add_column("id", "Int64", false)
        .add_column("writer", "Int64", false)
        .chunk_rows(u64::MAX)
        .with_primary_key_as_row_id("id")
        .build();
    db.create_table(config).unwrap();

    let db = Arc::new(db);
    let writers = 4usize;
    let rows_per_writer = 1_000i64;
    let barrier = Arc::new(Barrier::new(writers));
    let mut handles = Vec::new();

    for writer in 0..writers {
        let db = db.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let start = writer as i64 * rows_per_writer;
            let end = start + rows_per_writer;
            let batch = RecordBatch::try_from_iter(vec![
                ("id", Arc::new(Int64Array::from_iter_values(start..end)) as _),
                (
                    "writer",
                    Arc::new(Int64Array::from_iter_values(
                        (0..rows_per_writer).map(|_| writer as i64),
                    )) as _,
                ),
            ])
            .unwrap();
            barrier.wait();
            db.insert("concurrent", &batch).unwrap();
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }

    let count = db.select_all("concurrent").count().await.unwrap();
    assert_eq!(count, writers as i64 * rows_per_writer);
    for writer in 0..writers {
        let count = db
            .select_all("concurrent")
            .filter(Filter::eq("writer", writer as i64))
            .count()
            .await
            .unwrap();
        assert_eq!(count, rows_per_writer);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_and_insert_cannot_overwrite_each_other() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    let mut db = ChunkDb::open(path).unwrap();
    let config = TableBuilder::new("compact_insert", path)
        .add_column("id", "Int64", false)
        .add_column("value", "Int64", false)
        .chunk_rows(1_000)
        .with_snowflake_id()
        .build();
    db.create_table(config).unwrap();

    let seed = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int64Array::from_iter_values(0..100)) as _),
        ("value", Arc::new(Int64Array::from_iter_values(0..100)) as _),
    ])
    .unwrap();
    db.insert("compact_insert", &seed).unwrap();
    let update = RecordBatch::try_from_iter(vec![
        ("__row_id", Arc::new(UInt64Array::from(vec![5])) as _),
        ("id", Arc::new(Int64Array::from(vec![5])) as _),
        ("value", Arc::new(Int64Array::from(vec![-5])) as _),
    ])
    .unwrap();
    db.update_rows("compact_insert", &update).unwrap();

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(3));
    let insert_db = db.clone();
    let insert_barrier = barrier.clone();
    let insert = std::thread::spawn(move || {
        let batch = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from_iter_values(100..200)) as _),
            ("value", Arc::new(Int64Array::from_iter_values(100..200)) as _),
        ])
        .unwrap();
        insert_barrier.wait();
        insert_db.insert("compact_insert", &batch).unwrap();
    });
    let compact_db = db.clone();
    let compact_barrier = barrier.clone();
    let compact = std::thread::spawn(move || {
        compact_barrier.wait();
        compact_db.compact("compact_insert").unwrap();
    });
    barrier.wait();
    insert.join().unwrap();
    compact.join().unwrap();

    assert_eq!(db.select_all("compact_insert").count().await.unwrap(), 200);
    assert_eq!(
        db.select_all("compact_insert")
            .filter(Filter::eq("id", 5))
            .sum("value")
            .await
            .unwrap(),
        -5
    );
}

#[tokio::test]
async fn partition_key_update_that_moves_cells_is_rejected_explicitly() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    let mut db = ChunkDb::open(path).unwrap();

    let config = TableBuilder::new("partition_update", path)
        .add_column("id", "Int64", false)
        .add_column("sensor", "Utf8", false)
        .add_column("value", "Int64", false)
        .chunk_rows(u64::MAX)
        .add_hash_dimension("sensor", 32)
        .with_primary_key_as_row_id("id")
        .build();
    db.create_table(config).unwrap();

    let seed = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int64Array::from(vec![1])) as _),
        ("sensor", Arc::new(StringArray::from(vec!["old-sensor"])) as _),
        ("value", Arc::new(Int64Array::from(vec![10])) as _),
    ])
    .unwrap();
    db.insert("partition_update", &seed).unwrap();

    let old_bucket = chunk_db::HashRegistry::raw_hash_string("old-sensor") % 32;
    let new_sensor = (0..10_000)
        .map(|index| format!("new-sensor-{index}"))
        .find(|value| chunk_db::HashRegistry::raw_hash_string(value) % 32 != old_bucket)
        .unwrap();
    let update = RecordBatch::try_from_iter(vec![
        (
            "__row_id",
            Arc::new(UInt64Array::from(vec![int64_row_id(1)])) as _,
        ),
        ("id", Arc::new(Int64Array::from(vec![1])) as _),
        ("sensor", Arc::new(StringArray::from(vec![new_sensor.as_str()])) as _),
        ("value", Arc::new(Int64Array::from(vec![20])) as _),
    ])
    .unwrap();
    let error = db.update_rows("partition_update", &update).unwrap_err();
    assert!(error.to_string().contains("delete_rows + insert"));

    let old_count = db
        .select_all("partition_update")
        .filter(Filter::eq("sensor", "old-sensor"))
        .count()
        .await
        .unwrap();
    assert_eq!(old_count, 1, "rejected update must leave the row unchanged");
    let new_count = db
        .select_all("partition_update")
        .filter(Filter::eq("sensor", new_sensor))
        .count()
        .await
        .unwrap();
    assert_eq!(new_count, 0);
}
