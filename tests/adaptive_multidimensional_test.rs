use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use chunk_db::storage::parse_chunk_filename;
use chunk_db::{ChunkDb, Filter, TableBuilder};

fn hash_batch(start: i64, end: i64) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int64Array::from_iter_values(start..end)) as _,
        ),
        (
            "sensor",
            Arc::new(StringArray::from_iter_values(
                (start..end).map(|value| format!("sensor-{value:04}")),
            )) as _,
        ),
        (
            "value",
            Arc::new(Int64Array::from_iter_values(
                (start..end).map(|value| value * 10),
            )) as _,
        ),
    ])
    .unwrap()
}

fn live_coordinates(db: &ChunkDb, table: &str) -> Vec<chunk_db::ChunkCoordinate> {
    db.live_chunk_files(table)
        .unwrap()
        .into_iter()
        .map(|name| parse_chunk_filename(&name).unwrap().0)
        .collect()
}

#[tokio::test]
async fn hash_axis_splits_locally_persists_and_merges_after_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();

    {
        let mut db = ChunkDb::open(path).unwrap();
        let config = TableBuilder::new("hash_grid", path)
            .add_column("id", "Int64", false)
            .add_column("sensor", "Utf8", false)
            .add_column("value", "Int64", false)
            .chunk_rows(1_000_000)
            .max_cell_rows(50)
            .add_hash_dimension("sensor", 1)
            .add_column_group(vec!["id", "sensor"])
            .add_column_group(vec!["value"])
            .with_snowflake_id()
            .build();
        db.create_table(config).unwrap();
        db.insert("hash_grid", &hash_batch(0, 400)).unwrap();

        assert_eq!(db.select_all("hash_grid").count().await.unwrap(), 400);
        assert_eq!(
            db.select_all("hash_grid")
                .filter(Filter::eq("sensor", "sensor-0007"))
                .sum("value")
                .await
                .unwrap(),
            70
        );

        let coordinates = live_coordinates(&db, "hash_grid");
        assert!(coordinates.iter().any(|coord| coord.hash_levels[0] > 0));
        assert!(coordinates.iter().all(|coord| coord.range_levels.is_empty()));
        let stats = db.adaptive_grid_stats("hash_grid").unwrap();
        assert!(stats.hash_splits > 0);
        assert_eq!(stats.range_splits, 0);
        assert!(stats.max_hash_level > 0);
    }

    // Both the catalog coordinates and the local routing tree survive reopen.
    let db = ChunkDb::open(path).unwrap();
    assert_eq!(
        db.select_all("hash_grid")
            .filter(Filter::eq("sensor", "sensor-0399"))
            .count()
            .await
            .unwrap(),
        1
    );
    db.insert("hash_grid", &hash_batch(400, 500)).unwrap();
    assert_eq!(db.select_all("hash_grid").count().await.unwrap(), 500);

    // Leave twenty rows. Compaction materializes deletes, then coalesces small
    // local siblings; row-axis cells are intentionally outside this policy.
    let removed: Vec<u64> = (20..500).collect();
    db.delete_rows("hash_grid", &removed).unwrap();
    let before = db.live_chunk_files("hash_grid").unwrap().len();
    let compacted = db.compact("hash_grid").unwrap();
    let after = db.live_chunk_files("hash_grid").unwrap().len();
    assert!(compacted.cells_merged > 0);
    assert!(after < before, "merge should reduce live rectangles");
    assert_eq!(db.select_all("hash_grid").count().await.unwrap(), 20);
    assert_eq!(
        db.select_all("hash_grid")
            .filter(Filter::eq("sensor", "sensor-0007"))
            .count()
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn range_axis_splits_locally_and_level_aware_pruning_is_exact() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    let mut db = ChunkDb::open(path).unwrap();
    let config = TableBuilder::new("range_grid", path)
        .add_column("id", "Int64", false)
        .add_column("timestamp", "Int64", false)
        .add_column("value", "Int64", false)
        .chunk_rows(1_000_000)
        .max_cell_rows(40)
        .add_range_dimension("timestamp", 1024)
        .with_snowflake_id()
        .build();
    db.create_table(config).unwrap();

    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int64Array::from_iter_values(0..256)) as _),
        (
            "timestamp",
            Arc::new(Int64Array::from_iter_values(0..256)) as _,
        ),
        (
            "value",
            Arc::new(Int64Array::from_iter_values((0..256).map(|v| v * 2))) as _,
        ),
    ])
    .unwrap();
    db.insert("range_grid", &batch).unwrap();

    let coordinates = live_coordinates(&db, "range_grid");
    assert!(coordinates.iter().any(|coord| coord.range_levels[0] > 0));
    let stats = db.adaptive_grid_stats("range_grid").unwrap();
    assert!(stats.range_splits > 0);
    assert_eq!(stats.hash_splits, 0);
    assert_eq!(
        db.select_all("range_grid")
            .filter(Filter::between("timestamp", 75, 124))
            .count()
            .await
            .unwrap(),
        50
    );
    assert_eq!(
        db.select_all("range_grid")
            .filter(Filter::eq("timestamp", 127))
            .sum("value")
            .await
            .unwrap(),
        254
    );

    drop(db);
    let reopened = ChunkDb::open(path).unwrap();
    assert_eq!(
        reopened
            .select_all("range_grid")
            .filter(Filter::between("timestamp", 200, 255))
            .count()
            .await
            .unwrap(),
        56
    );
}

#[test]
fn adaptive_files_are_sorted_and_use_bounded_row_groups() {
    use arrow::array::Array;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    let mut db = ChunkDb::open(path).unwrap();
    let config = TableBuilder::new("physical", path)
        .add_column("id", "Int64", false)
        .add_column("sensor", "Utf8", false)
        .add_column("value", "Int64", false)
        .chunk_rows(1_000_000)
        .max_cell_rows(64)
        .add_hash_dimension("sensor", 1)
        .with_snowflake_id()
        .build();
    db.create_table(config).unwrap();
    db.insert("physical", &hash_batch(0, 200)).unwrap();

    for filename in db.live_chunk_files("physical").unwrap() {
        let file_path = Path::new(path).join("physical").join("chunks").join(filename);
        let file = std::fs::File::open(file_path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        assert!(builder.metadata().row_groups().iter()
            .all(|group| group.num_rows() <= 64));
        let batches = builder.build().unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        if batches.is_empty() {
            continue;
        }
        let batch = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let sensor_idx = batch.schema().index_of("sensor").unwrap();
        let sensors = batch.column(sensor_idx).as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 1..sensors.len() {
            assert!(sensors.value(index - 1) <= sensors.value(index));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_share_one_local_grid_and_reopen_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let mut db = ChunkDb::open(&path).unwrap();
    let config = TableBuilder::new("concurrent_grid", &path)
        .add_column("id", "Int64", false)
        .add_column("sensor", "Utf8", false)
        .add_column("value", "Int64", false)
        .chunk_rows(1_000_000)
        .max_cell_rows(30)
        .add_hash_dimension("sensor", 1)
        .with_snowflake_id()
        .build();
    db.create_table(config).unwrap();
    let db = Arc::new(db);

    let tasks: Vec<_> = (0..3)
        .map(|writer| {
            let db = db.clone();
            tokio::task::spawn_blocking(move || {
                let start = writer * 60;
                db.insert("concurrent_grid", &hash_batch(start, start + 60))
                    .unwrap();
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }

    assert_eq!(db.select_all("concurrent_grid").count().await.unwrap(), 180);
    assert!(db.adaptive_grid_stats("concurrent_grid").unwrap().hash_splits > 0);
    drop(db);

    let reopened = ChunkDb::open(&path).unwrap();
    assert_eq!(
        reopened.select_all("concurrent_grid").count().await.unwrap(),
        180
    );
}
