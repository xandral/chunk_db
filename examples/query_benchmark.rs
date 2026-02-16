/// Simple query benchmark with configurable setup
///
/// Modify the QUERY section to test different queries.
/// All configuration is at the top of main() for easy tuning.

use chunk_db::{ChunkDb, TableBuilder, RecordBatch, Filter};
use arrow::array::{Int64Array, StringArray, BooleanArray, Float64Array};
use std::sync::Arc;
use std::hint::black_box;
use std::time::Instant;

#[tokio::main]
async fn main() {
    println!("=================================");
    println!("ChunkDB Simple Query Benchmark");
    println!("=================================\n");

    // ========================================
    // CONFIGURATION - Modify these parameters
    // ========================================

    let db_path = "/tmp/benchmark_simple";
    let table_name = "events";

    // Data volume
    let num_rows = 1_000_000;           // Total rows to insert
    let num_sensors = 100;                // Number of unique sensors
    let batch_size = 10_000;             // Rows per insert batch

    // Partitioning configuration
    let chunk_rows = 10_000;             // Rows per row bucket
    let hash_buckets = 10;               // Hash buckets for sensor_id

    // Column groups (vertical partitioning)
    // Empty = all columns in one group
    // Example: vec![vec!["timestamp", "sensor_id"], vec!["temperature", "pressure"]]
    let column_groups: Vec<Vec<&str>> = vec![];

    // Range dimensions (besides implicit row_bucket)
    // Example: vec![("timestamp", 86400)] for daily buckets
    let range_dimensions: Vec<(&str, u64)> = vec![];

    println!("Configuration:");
    println!("  Rows:          {}", num_rows);
    println!("  Sensors:       {}", num_sensors);
    println!("  Chunk rows:    {}", chunk_rows);
    println!("  Hash buckets:  {}", hash_buckets);
    println!("  Column groups: {}", if column_groups.is_empty() { "1 (all columns)".to_string() } else { column_groups.len().to_string() });
    println!("  Range dims:    {}", range_dimensions.len());
    println!();

    // ========================================
    // SETUP: Create database and table
    // ========================================

    println!("Setting up database...");
    std::fs::remove_dir_all(db_path).ok();
    let mut db = ChunkDb::open(db_path).unwrap();

    let mut builder = TableBuilder::new(table_name, db_path)
        .add_column("timestamp", "Int64", false)
        .add_column("sensor_id", "Utf8", false)
        .add_column("temperature", "Float64", true)
        .add_column("pressure", "Float64", true)
        .add_column("humidity", "Int64", true)
        .add_column("status", "Bool", true)
        .chunk_rows(chunk_rows)
        .add_hash_dimension("sensor_id", hash_buckets)
        .with_primary_key_as_row_id("timestamp");

    // Add range dimensions if specified
    for (col, size) in range_dimensions {
        builder = builder.add_range_dimension(col, size);
    }

    // Add column groups if specified
    for group in column_groups {
        builder = builder.add_column_group(group);
    }

    let config = builder.build();
    db.create_table(config).unwrap();

    println!("✓ Table created");

    // ========================================
    // DATA INSERTION
    // ========================================

    println!("\nInserting {} rows in batches of {}...", num_rows, batch_size);
    let insert_start = Instant::now();

    let mut inserted = 0;
    while inserted < num_rows {
        let batch_rows = batch_size.min(num_rows - inserted);
        let start = inserted as i64;
        let end = start + batch_rows as i64;

        let batch = RecordBatch::try_from_iter(vec![
            ("timestamp", Arc::new(Int64Array::from_iter_values(start..end)) as _),
            ("sensor_id", Arc::new(StringArray::from(
                (start..end).map(|i| format!("sensor_{}", i % num_sensors as i64)).collect::<Vec<_>>()
            )) as _),
            ("temperature", Arc::new(Float64Array::from(
                (start..end).map(|i| 20.0 + (i % 100) as f64 * 0.5).collect::<Vec<_>>()
            )) as _),
            ("pressure", Arc::new(Float64Array::from(
                (start..end).map(|i| 1000.0 + (i % 200) as f64).collect::<Vec<_>>()
            )) as _),
            ("humidity", Arc::new(Int64Array::from_iter_values(
                (start..end).map(|i| 30 + (i % 70))
            )) as _),
            ("status", Arc::new(BooleanArray::from(
                (start..end).map(|i| i % 3 == 0).collect::<Vec<_>>()
            )) as _),
        ]).unwrap();

        db.insert(table_name, &batch).unwrap();
        inserted += batch_rows;

        if inserted % 100_000 == 0 {
            print!("  Inserted: {}/{}\r", inserted, num_rows);
        }
    }

    let insert_duration = insert_start.elapsed();
    println!("\n✓ Inserted {} rows in {:.2}s ({:.0} rows/sec)",
        num_rows,
        insert_duration.as_secs_f64(),
        num_rows as f64 / insert_duration.as_secs_f64()
    );

    // ========================================
    // QUERY BENCHMARK
    // ========================================

    println!("\n=================================");
    println!("RUNNING QUERY BENCHMARK");
    println!("=================================\n");

    // Warm-up query
    println!("Warming up...");
    let _ = db.select_all(table_name).count().await.unwrap();
    println!("✓ Warm-up complete\n");

    // ========================================
    // MODIFY THIS SECTION TO TEST YOUR QUERY
    // ========================================

    // println!("Query: Full scan count");
    // benchmark_query(|| async {
    //     db.select_all(table_name)
    //         .count()
    //         .await
    //         .unwrap()
    // }).await;

    println!("\nQuery: Single sensor filter + count");
    benchmark_query(|| async {
        db.select_all(table_name)
            .filter(Filter::eq("sensor_id", "sensor_0"))
            .count()
            .await
            .unwrap()
    }).await;

    println!("\nQuery: Time range filter + count");
    benchmark_query(|| async {
        db.select_all(table_name)
            .filter(Filter::between("timestamp", 100_000, 150_000))
            .count()
            .await
            .unwrap()
    }).await;

    println!("\nQuery: Combined filters (sensor + time range) + count");
    benchmark_query(|| async {
        db.select_all(table_name)
            .filter(Filter::eq("sensor_id", "sensor_5"))
            .filter(Filter::between("timestamp", 0, 100_000))
            .count()
            .await
            .unwrap()
    }).await;

    // println!("\nQuery: Aggregation (avg temperature for sensor)");
    // benchmark_query(|| async {
    //     db.select_all(table_name)
    //         .filter(Filter::eq("sensor_id", "sensor_3"))
    //         .avg("temperature")
    //         .await
    //         .unwrap()
    // }).await;

    // println!("\nQuery: Column projection (2 columns)");
    // benchmark_query(|| async {
    //     let results = db.select(&["timestamp", "temperature"])
    //         .from(table_name)
    //         .filter(Filter::eq("sensor_id", "sensor_7"))
    //         .execute()
    //         .await
    //         .unwrap();
    //     results.iter().map(|b| b.num_rows()).sum::<usize>()
    // }).await;

    // println!("\nQuery: Complex multi-filter");
    // benchmark_query(|| async {
    //     db.select_all(table_name)
    //         .filter(Filter::eq("sensor_id", "sensor_2"))
    //         .filter(Filter::between("timestamp", 250_000, 750_000))
    //         .filter(Filter::eq("status", true))
    //         .count()
    //         .await
    //         .unwrap()
    // }).await;

    // ========================================
    // ADD YOUR CUSTOM QUERIES HERE
    // ========================================

    // Example template:
    // println!("\nQuery: Your custom query description");
    // benchmark_query(|| async {
    //     db.select_all(table_name)
    //         .filter(Filter::eq("sensor_id", "sensor_0"))
    //         // Add your filters/aggregations here
    //         .count()
    //         .await
    //         .unwrap()
    // }).await;

    println!("\n=================================");
    println!("BENCHMARK COMPLETE");
    println!("=================================");
}

/// Run a query multiple times and report statistics
async fn benchmark_query<F, Fut, T>(query_fn: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = T>,
    T: std::fmt::Display,
{
    let warmup_runs = 2;
    let bench_runs = 10;

    // Warmup
    for _ in 0..warmup_runs {
        let _ = query_fn().await;
    }

    // Benchmark
    let mut times = Vec::new();
    let mut result = None;

    for _ in 0..bench_runs {
        let start = Instant::now();
        let res = black_box(query_fn().await);
        let duration = start.elapsed();
        times.push(duration);
        result = Some(res);
    }

    // Statistics
    times.sort();
    let min = times[0];
    let max = times[times.len() - 1];
    let median = times[times.len() / 2];
    let mean = times.iter().sum::<std::time::Duration>() / times.len() as u32;

    println!("  Result: {}", result.unwrap());
    println!("  Runs:   {}", bench_runs);
    println!("  Min:    {:.2}ms", min.as_secs_f64() * 1000.0);
    println!("  Median: {:.2}ms", median.as_secs_f64() * 1000.0);
    println!("  Mean:   {:.2}ms", mean.as_secs_f64() * 1000.0);
    println!("  Max:    {:.2}ms", max.as_secs_f64() * 1000.0);
}
