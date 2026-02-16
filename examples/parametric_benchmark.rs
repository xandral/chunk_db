/// Parametric benchmark suite for ChunkDB
///
/// Tests all combinations of:
/// - chunk_rows: 5000 to 50000 (step 5000)
/// - hash_buckets: 10 to 100 (step 10)
/// - range_dimension_size: 5000 to 50000 (step 5000)
///
/// Results are saved to CSV for analysis and plotting.

use chunk_db::{ChunkDb, TableBuilder, RecordBatch, Filter};
use arrow::array::{Int64Array, StringArray, BooleanArray, Float64Array};
use std::sync::Arc;
use std::hint::black_box;
use std::time::Instant;
use std::fs::File;
use std::io::Write;

#[derive(Clone)]
struct BenchmarkConfig {
    chunk_rows: u64,
    hash_buckets: u64,
    range_dim_size: u64,
}

#[derive(Clone)]
struct QueryResult {
    config: BenchmarkConfig,
    query_name: String,
    result_value: i64,
    min_ms: f64,
    median_ms: f64,
    mean_ms: f64,
    max_ms: f64,
}

#[tokio::main]
async fn main() {
    println!("=====================================");
    println!("ChunkDB Parametric Benchmark Suite");
    println!("=====================================\n");

    // Fixed parameters
    let num_rows = 1_000_000;
    let num_sensors = 100;
    let batch_size = 10_000;

    // Parameter ranges
    let chunk_rows_range: Vec<u64> = (1..=10).map(|i| i * 5000).collect();
    let hash_buckets_range: Vec<u64> = (1..=10).map(|i| i * 10).collect();
    let range_dim_range: Vec<u64> = (1..=10).map(|i| i * 5000).collect();

    println!("Test parameters:");
    println!("  Data size:      {} rows", num_rows);
    println!("  Sensors:        {}", num_sensors);
    println!("  chunk_rows:     {:?}", chunk_rows_range);
    println!("  hash_buckets:   {:?}", hash_buckets_range);
    println!("  range_dim_size: {:?}", range_dim_range);

    let total_configs = chunk_rows_range.len() * hash_buckets_range.len() * range_dim_range.len();
    println!("\n  Total configs:  {}", total_configs);
    println!("  Queries/config: 3");
    println!("  Total tests:    {}\n", total_configs * 3);

    let mut all_results = Vec::new();
    let mut config_idx = 0;

    let start_time = Instant::now();

    // Iterate over all parameter combinations
    for &chunk_rows in &chunk_rows_range {
        for &hash_buckets in &hash_buckets_range {
            for &range_dim_size in &range_dim_range {
                config_idx += 1;

                let config = BenchmarkConfig {
                    chunk_rows,
                    hash_buckets,
                    range_dim_size,
                };

                println!("─────────────────────────────────────");
                println!("Config {}/{}: chunk_rows={}, hash_buckets={}, range_dim={}",
                    config_idx, total_configs, chunk_rows, hash_buckets, range_dim_size);
                println!("─────────────────────────────────────");

                // Run benchmark for this configuration
                let results = run_benchmark_config(
                    &config,
                    num_rows,
                    num_sensors,
                    batch_size
                ).await;

                all_results.extend(results);

                // Show progress
                let elapsed = start_time.elapsed().as_secs();
                let avg_per_config = elapsed as f64 / config_idx as f64;
                let remaining = ((total_configs - config_idx) as f64 * avg_per_config) as u64;
                println!("Progress: {}/{} ({:.1}%) - Elapsed: {}s - ETA: {}s\n",
                    config_idx, total_configs,
                    (config_idx as f64 / total_configs as f64) * 100.0,
                    elapsed, remaining);
            }
        }
    }

    println!("=====================================");
    println!("Saving results...");
    println!("=====================================\n");

    // Save results to CSV
    save_results_csv(&all_results, "benchmark_results.csv").unwrap();

    // Generate summary
    print_summary(&all_results);
    save_summary(&all_results, "benchmark_summary.txt").unwrap();

    println!("\n✓ Results saved to:");
    println!("  - benchmark_results.csv (detailed results)");
    println!("  - benchmark_summary.txt (summary)");
    println!("\nTotal time: {:.1}s", start_time.elapsed().as_secs_f64());
    println!("\nRun the plotting script to generate graphs:");
    println!("  python3 plot_benchmark.py");
}

async fn run_benchmark_config(
    config: &BenchmarkConfig,
    num_rows: usize,
    num_sensors: usize,
    batch_size: usize,
) -> Vec<QueryResult> {
    let db_path = "/tmp/parametric_benchmark";
    let table_name = "events";

    // Setup database
    std::fs::remove_dir_all(db_path).ok();
    let mut db = ChunkDb::open(db_path).unwrap();

    let builder = TableBuilder::new(table_name, db_path)
        .add_column("timestamp", "Int64", false)
        .add_column("sensor_id", "Utf8", false)
        .add_column("temperature", "Float64", true)
        .add_column("pressure", "Float64", true)
        .add_column("humidity", "Int64", true)
        .add_column("status", "Bool", true)
        .chunk_rows(config.chunk_rows)
        .add_hash_dimension("sensor_id", config.hash_buckets)
        .add_range_dimension("timestamp", config.range_dim_size)
        .with_primary_key_as_row_id("timestamp");

    let table_config = builder.build();
    db.create_table(table_config).unwrap();

    // Insert data
    print!("  Inserting data... ");
    std::io::stdout().flush().unwrap();

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
    }
    println!("✓");

    // Warmup
    let _ = db.select_all(table_name).count().await.unwrap();

    let mut results = Vec::new();

    // Query 1: Single sensor filter
    print!("  Q1: Single sensor... ");
    std::io::stdout().flush().unwrap();
    let stats = benchmark_query(5, || async {
        db.select_all(table_name)
            .filter(Filter::eq("sensor_id", "sensor_0"))
            .count()
            .await
            .unwrap()
    }).await;
    println!("✓ {:.2}ms", stats.median_ms);

    results.push(QueryResult {
        config: config.clone(),
        query_name: "single_sensor".to_string(),
        result_value: stats.result,
        min_ms: stats.min_ms,
        median_ms: stats.median_ms,
        mean_ms: stats.mean_ms,
        max_ms: stats.max_ms,
    });

    // Query 2: Time range filter
    print!("  Q2: Time range... ");
    std::io::stdout().flush().unwrap();
    let stats = benchmark_query(5, || async {
        db.select_all(table_name)
            .filter(Filter::between("timestamp", 100_000, 200_000))
            .count()
            .await
            .unwrap()
    }).await;
    println!("✓ {:.2}ms", stats.median_ms);

    results.push(QueryResult {
        config: config.clone(),
        query_name: "time_range".to_string(),
        result_value: stats.result,
        min_ms: stats.min_ms,
        median_ms: stats.median_ms,
        mean_ms: stats.mean_ms,
        max_ms: stats.max_ms,
    });

    // Query 3: Combined filters
    print!("  Q3: Combined... ");
    std::io::stdout().flush().unwrap();
    let stats = benchmark_query(5, || async {
        db.select_all(table_name)
            .filter(Filter::eq("sensor_id", "sensor_5"))
            .filter(Filter::between("timestamp", 0, 100_000))
            .count()
            .await
            .unwrap()
    }).await;
    println!("✓ {:.2}ms", stats.median_ms);

    results.push(QueryResult {
        config: config.clone(),
        query_name: "combined".to_string(),
        result_value: stats.result,
        min_ms: stats.min_ms,
        median_ms: stats.median_ms,
        mean_ms: stats.mean_ms,
        max_ms: stats.max_ms,
    });

    results
}

struct QueryStats {
    result: i64,
    min_ms: f64,
    median_ms: f64,
    mean_ms: f64,
    max_ms: f64,
}

async fn benchmark_query<F, Fut>(runs: usize, query_fn: F) -> QueryStats
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = i64>,
{
    // 1 warmup run (not counted)
    let _ = black_box(query_fn().await);

    let mut times = Vec::new();
    let mut result: i64 = 0;

    for _ in 0..runs {
        let start = Instant::now();
        result = black_box(query_fn().await);
        times.push(start.elapsed());
    }

    times.sort();
    let min = times[0];
    let max = times[times.len() - 1];
    let median = times[times.len() / 2];
    let mean = times.iter().sum::<std::time::Duration>() / times.len() as u32;

    QueryStats {
        result,
        min_ms: min.as_secs_f64() * 1000.0,
        median_ms: median.as_secs_f64() * 1000.0,
        mean_ms: mean.as_secs_f64() * 1000.0,
        max_ms: max.as_secs_f64() * 1000.0,
    }
}

fn save_results_csv(results: &[QueryResult], filename: &str) -> std::io::Result<()> {
    let mut file = File::create(filename)?;

    // Header
    writeln!(file, "chunk_rows,hash_buckets,range_dim_size,query,result,min_ms,median_ms,mean_ms,max_ms")?;

    // Data
    for r in results {
        writeln!(
            file,
            "{},{},{},{},{},{:.4},{:.4},{:.4},{:.4}",
            r.config.chunk_rows,
            r.config.hash_buckets,
            r.config.range_dim_size,
            r.query_name,
            r.result_value,
            r.min_ms,
            r.median_ms,
            r.mean_ms,
            r.max_ms
        )?;
    }

    Ok(())
}

fn print_summary(results: &[QueryResult]) {
    println!("\n=====================================");
    println!("BENCHMARK SUMMARY");
    println!("=====================================\n");

    for query in ["single_sensor", "time_range", "combined"] {
        let query_results: Vec<_> = results.iter()
            .filter(|r| r.query_name == query)
            .collect();

        if query_results.is_empty() {
            continue;
        }

        let median_times: Vec<f64> = query_results.iter().map(|r| r.median_ms).collect();
        let min_time = median_times.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_time = median_times.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let avg_time = median_times.iter().sum::<f64>() / median_times.len() as f64;

        let best = query_results.iter().min_by(|a, b|
            a.median_ms.partial_cmp(&b.median_ms).unwrap()
        ).unwrap();

        let worst = query_results.iter().max_by(|a, b|
            a.median_ms.partial_cmp(&b.median_ms).unwrap()
        ).unwrap();

        println!("Query: {}", query);
        println!("  Configurations tested: {}", query_results.len());
        println!("  Min time:  {:.2}ms", min_time);
        println!("  Max time:  {:.2}ms", max_time);
        println!("  Avg time:  {:.2}ms", avg_time);
        println!("  Speedup:   {:.2}x (best vs worst)", worst.median_ms / best.median_ms);
        println!("  Best config:  chunk_rows={}, hash_buckets={}, range_dim={}",
            best.config.chunk_rows, best.config.hash_buckets, best.config.range_dim_size);
        println!("  Worst config: chunk_rows={}, hash_buckets={}, range_dim={}",
            worst.config.chunk_rows, worst.config.hash_buckets, worst.config.range_dim_size);
        println!();
    }
}

fn save_summary(results: &[QueryResult], filename: &str) -> std::io::Result<()> {
    let mut file = File::create(filename)?;

    writeln!(file, "ChunkDB Parametric Benchmark Summary")?;
    writeln!(file, "====================================\n")?;

    for query in ["single_sensor", "time_range", "combined"] {
        let query_results: Vec<_> = results.iter()
            .filter(|r| r.query_name == query)
            .collect();

        if query_results.is_empty() {
            continue;
        }

        let median_times: Vec<f64> = query_results.iter().map(|r| r.median_ms).collect();
        let min_time = median_times.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_time = median_times.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let avg_time = median_times.iter().sum::<f64>() / median_times.len() as f64;

        let best = query_results.iter().min_by(|a, b|
            a.median_ms.partial_cmp(&b.median_ms).unwrap()
        ).unwrap();

        let worst = query_results.iter().max_by(|a, b|
            a.median_ms.partial_cmp(&b.median_ms).unwrap()
        ).unwrap();

        writeln!(file, "Query: {}", query)?;
        writeln!(file, "  Configurations tested: {}", query_results.len())?;
        writeln!(file, "  Min time:  {:.2}ms", min_time)?;
        writeln!(file, "  Max time:  {:.2}ms", max_time)?;
        writeln!(file, "  Avg time:  {:.2}ms", avg_time)?;
        writeln!(file, "  Speedup:   {:.2}x (best vs worst)", worst.median_ms / best.median_ms)?;
        writeln!(file, "  Best config:  chunk_rows={}, hash_buckets={}, range_dim={}",
            best.config.chunk_rows, best.config.hash_buckets, best.config.range_dim_size)?;
        writeln!(file, "  Worst config: chunk_rows={}, hash_buckets={}, range_dim={}",
            worst.config.chunk_rows, worst.config.hash_buckets, worst.config.range_dim_size)?;
        writeln!(file)?;
    }

    Ok(())
}
