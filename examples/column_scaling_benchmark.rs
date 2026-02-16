/// Column Scaling Benchmark - Test performance vs number of columns selected
///
/// For each query type (sensor, time_range, combined):
/// - Test with 1, 3, 7, 15, 31, 63, 127 columns
/// - Measure how performance scales with column count
/// - Analyze column projection overhead

use chunk_db::{ChunkDb, TableBuilder, RecordBatch, Filter};
use arrow::array::{Int64Array, StringArray, Float64Array};
use std::sync::Arc;
use std::hint::black_box;
use std::time::Instant;
use std::fs::File;
use std::io::Write as IoWrite;

#[derive(Clone)]
struct BenchmarkConfig {
    chunk_rows: u64,
    hash_buckets: u64,
    range_dim_size: u64,
}

#[derive(Clone)]
struct QueryResult {
    config: BenchmarkConfig,
    query_type: String,
    num_columns: usize,
    result_rows: i64,
    min_ms: f64,
    median_ms: f64,
    mean_ms: f64,
    max_ms: f64,
}

#[tokio::main]
async fn main() {
    println!("================================================");
    println!("ChunkDB Column Scaling Benchmark");
    println!("================================================\n");

    // Fixed parameters
    let num_rows = 1_000_000;
    let num_sensors = 100;
    let batch_size = 10_000;

    // Parameter ranges
    let chunk_rows_range: Vec<u64> = vec![5000, 25000, 50000];
    let hash_buckets_range: Vec<u64> = vec![10, 50, 100];
    let range_dim_range: Vec<u64> = vec![5000, 25000, 50000];

    // Column counts to test
    let column_counts = vec![1, 3, 7, 15, 31, 63, 127];

    println!("Test parameters:");
    println!("  Data size:      {} rows", num_rows);
    println!("  Columns:        128 data columns");
    println!("  Column groups:  4 groups (~40 cols each)");
    println!("  Sensors:        {}", num_sensors);
    println!("  chunk_rows:     {:?}", chunk_rows_range);
    println!("  hash_buckets:   {:?}", hash_buckets_range);
    println!("  range_dim_size: {:?}", range_dim_range);
    println!("  Column counts:  {:?}", column_counts);

    let total_configs = chunk_rows_range.len() * hash_buckets_range.len() * range_dim_range.len();
    let total_tests = total_configs * 3 * column_counts.len(); // 3 query types × N column counts

    println!("\n  Total configs:  {}", total_configs);
    println!("  Query types:    3 (sensor, time_range, combined)");
    println!("  Column tests:   {} per query type", column_counts.len());
    println!("  Total tests:    {}\n", total_tests);

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
                    batch_size,
                    &column_counts
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

    // Generate summary and scaling analysis
    print_summary(&all_results);
    save_summary(&all_results, "benchmark_summary.txt").unwrap();

    // Generate scaling analysis
    analyze_scaling(&all_results, "benchmark_scaling_analysis.txt").unwrap();

    println!("\n✓ Results saved to:");
    println!("  - benchmark_results.csv (detailed results)");
    println!("  - benchmark_summary.txt (summary)");
    println!("  - benchmark_scaling_analysis.txt (scaling analysis)");
    println!("\nTotal time: {:.1}s", start_time.elapsed().as_secs_f64());
    println!("\nRun the plotting script to generate graphs:");
    println!("  python3 plot_scaling.py");
}

async fn run_benchmark_config(
    config: &BenchmarkConfig,
    num_rows: usize,
    num_sensors: usize,
    batch_size: usize,
    column_counts: &[usize],
) -> Vec<QueryResult> {
    let db_path = "/tmp/column_scaling_benchmark";
    let table_name = "events";

    // Setup database
    std::fs::remove_dir_all(db_path).ok();
    let mut db = ChunkDb::open(db_path).unwrap();

    // Build table with 128 columns
    let mut builder = TableBuilder::new(table_name, db_path)
        .add_column("timestamp", "Int64", false)
        .add_column("sensor_id", "Utf8", false);

    // Add 128 data columns
    for i in 0..128 {
        builder = builder.add_column(&format!("col_{}", i), "Float64", true);
    }

    // Configure partitioning
    builder = builder
        .chunk_rows(config.chunk_rows)
        .add_hash_dimension("sensor_id", config.hash_buckets)
        .add_range_dimension("timestamp", config.range_dim_size)
        .with_primary_key_as_row_id("timestamp");

    // Add column groups of ~40 columns each
    let mut group0: Vec<String> = vec!["timestamp".to_string(), "sensor_id".to_string()];
    group0.extend((0..38).map(|i| format!("col_{}", i)));
    builder = builder.add_column_group(group0.iter().map(|s| s.as_str()).collect());

    let group1: Vec<String> = (38..78).map(|i| format!("col_{}", i)).collect();
    builder = builder.add_column_group(group1.iter().map(|s| s.as_str()).collect());

    let group2: Vec<String> = (78..118).map(|i| format!("col_{}", i)).collect();
    builder = builder.add_column_group(group2.iter().map(|s| s.as_str()).collect());

    let group3: Vec<String> = (118..128).map(|i| format!("col_{}", i)).collect();
    builder = builder.add_column_group(group3.iter().map(|s| s.as_str()).collect());

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

        let col_names: Vec<String> = (0..128).map(|i| format!("col_{}", i)).collect();

        let mut columns: Vec<(&str, Arc<dyn arrow::array::Array>)> = vec![
            ("timestamp", Arc::new(Int64Array::from_iter_values(start..end)) as _),
            ("sensor_id", Arc::new(StringArray::from(
                (start..end).map(|i| format!("sensor_{}", i % num_sensors as i64)).collect::<Vec<_>>()
            )) as _),
        ];

        for (i, col_name) in col_names.iter().enumerate() {
            let col_data = Arc::new(Float64Array::from(
                (start..end).map(|j| (j as f64 * (i + 1) as f64) % 1000.0).collect::<Vec<_>>()
            )) as _;
            columns.push((col_name.as_str(), col_data));
        }

        let batch = RecordBatch::try_from_iter(columns).unwrap();
        db.insert(table_name, &batch).unwrap();
        inserted += batch_rows;
    }
    println!("✓");

    // Warmup
    let _ = db.select(&["col_0"])
        .from(table_name)
        .filter(Filter::eq("sensor_id", "sensor_0"))
        .execute()
        .await
        .unwrap();

    let mut results = Vec::new();

    // Test each query type with varying column counts
    for &num_cols in column_counts {
        // Select columns spread across all groups
        let step = 128 / num_cols.max(1);
        let selected_cols: Vec<String> = (0..num_cols)
            .map(|i| format!("col_{}", (i * step).min(127)))
            .collect();
        let col_refs: Vec<&str> = selected_cols.iter().map(|s| s.as_str()).collect();

        // Query 1: Single sensor filter
        print!("  Q1: sensor + {} cols... ", num_cols);
        std::io::stdout().flush().unwrap();

        let stats = benchmark_query(5, || async {
            let result = db.select(&col_refs)
                .from(table_name)
                .filter(Filter::eq("sensor_id", "sensor_0"))
                .execute()
                .await
                .unwrap();
            result.iter().map(|b| b.num_rows() as i64).sum()
        }).await;
        println!("{:.2}ms", stats.median_ms);

        results.push(QueryResult {
            config: config.clone(),
            query_type: "single_sensor".to_string(),
            num_columns: num_cols,
            result_rows: stats.result,
            min_ms: stats.min_ms,
            median_ms: stats.median_ms,
            mean_ms: stats.mean_ms,
            max_ms: stats.max_ms,
        });

        // Query 2: Time range filter
        print!("  Q2: time_range + {} cols... ", num_cols);
        std::io::stdout().flush().unwrap();

        let stats = benchmark_query(5, || async {
            let result = db.select(&col_refs)
                .from(table_name)
                .filter(Filter::between("timestamp", 100_000, 200_000))
                .execute()
                .await
                .unwrap();
            result.iter().map(|b| b.num_rows() as i64).sum()
        }).await;
        println!("{:.2}ms", stats.median_ms);

        results.push(QueryResult {
            config: config.clone(),
            query_type: "time_range".to_string(),
            num_columns: num_cols,
            result_rows: stats.result,
            min_ms: stats.min_ms,
            median_ms: stats.median_ms,
            mean_ms: stats.mean_ms,
            max_ms: stats.max_ms,
        });

        // Query 3: Combined filters
        print!("  Q3: combined + {} cols... ", num_cols);
        std::io::stdout().flush().unwrap();

        let stats = benchmark_query(5, || async {
            let result = db.select(&col_refs)
                .from(table_name)
                .filter(Filter::eq("sensor_id", "sensor_5"))
                .filter(Filter::between("timestamp", 0, 100_000))
                .execute()
                .await
                .unwrap();
            result.iter().map(|b| b.num_rows() as i64).sum()
        }).await;
        println!("{:.2}ms", stats.median_ms);

        results.push(QueryResult {
            config: config.clone(),
            query_type: "combined".to_string(),
            num_columns: num_cols,
            result_rows: stats.result,
            min_ms: stats.min_ms,
            median_ms: stats.median_ms,
            mean_ms: stats.mean_ms,
            max_ms: stats.max_ms,
        });
    }

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
    writeln!(file, "chunk_rows,hash_buckets,range_dim_size,query_type,num_columns,result_rows,min_ms,median_ms,mean_ms,max_ms")?;

    for r in results {
        writeln!(
            file,
            "{},{},{},{},{},{},{:.4},{:.4},{:.4},{:.4}",
            r.config.chunk_rows,
            r.config.hash_buckets,
            r.config.range_dim_size,
            r.query_type,
            r.num_columns,
            r.result_rows,
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

    for query_type in ["single_sensor", "time_range", "combined"] {
        println!("Query Type: {}", query_type);
        println!("─────────────────────────────────────");

        let query_results: Vec<_> = results.iter()
            .filter(|r| r.query_type == query_type)
            .collect();

        // Group by num_columns
        let mut col_counts: Vec<usize> = query_results.iter().map(|r| r.num_columns).collect();
        col_counts.sort();
        col_counts.dedup();

        for num_cols in col_counts {
            let col_results: Vec<_> = query_results.iter()
                .filter(|r| r.num_columns == num_cols)
                .collect();

            let times: Vec<f64> = col_results.iter().map(|r| r.median_ms).collect();
            let min_time = times.iter().cloned().fold(f64::INFINITY, f64::min);
            let avg_time = times.iter().sum::<f64>() / times.len() as f64;

            println!("  {} cols: {:.2}ms (min) - {:.2}ms (avg)",
                num_cols, min_time, avg_time);
        }
        println!();
    }
}

fn save_summary(results: &[QueryResult], filename: &str) -> std::io::Result<()> {
    let mut file = File::create(filename)?;
    writeln!(file, "ChunkDB Column Scaling Benchmark Summary")?;
    writeln!(file, "=========================================\n")?;

    for query_type in ["single_sensor", "time_range", "combined"] {
        writeln!(file, "Query Type: {}", query_type)?;
        writeln!(file, "─────────────────────────────────────")?;

        let query_results: Vec<_> = results.iter()
            .filter(|r| r.query_type == query_type)
            .collect();

        let mut col_counts: Vec<usize> = query_results.iter().map(|r| r.num_columns).collect();
        col_counts.sort();
        col_counts.dedup();

        for num_cols in col_counts {
            let col_results: Vec<_> = query_results.iter()
                .filter(|r| r.num_columns == num_cols)
                .collect();

            let times: Vec<f64> = col_results.iter().map(|r| r.median_ms).collect();
            let min_time = times.iter().cloned().fold(f64::INFINITY, f64::min);
            let avg_time = times.iter().sum::<f64>() / times.len() as f64;

            writeln!(file, "  {} cols: {:.2}ms (min) - {:.2}ms (avg)",
                num_cols, min_time, avg_time)?;
        }
        writeln!(file)?;
    }

    Ok(())
}

fn analyze_scaling(results: &[QueryResult], filename: &str) -> std::io::Result<()> {
    let mut file = File::create(filename)?;
    writeln!(file, "Column Scaling Analysis")?;
    writeln!(file, "=======================")?;
    writeln!(file, "\nHow performance scales with number of columns:\n")?;

    for query_type in ["single_sensor", "time_range", "combined"] {
        writeln!(file, "Query: {}", query_type)?;
        writeln!(file, "───────────────────────")?;

        let query_results: Vec<_> = results.iter()
            .filter(|r| r.query_type == query_type)
            .collect();

        let mut col_counts: Vec<usize> = query_results.iter().map(|r| r.num_columns).collect();
        col_counts.sort();
        col_counts.dedup();

        if col_counts.len() >= 2 {
            let base_cols = col_counts[0];
            let base_results: Vec<_> = query_results.iter()
                .filter(|r| r.num_columns == base_cols)
                .collect();
            let base_avg = base_results.iter().map(|r| r.median_ms).sum::<f64>() / base_results.len() as f64;

            writeln!(file, "  Baseline ({} col): {:.2}ms", base_cols, base_avg)?;
            writeln!(file)?;

            for &num_cols in &col_counts[1..] {
                let col_results: Vec<_> = query_results.iter()
                    .filter(|r| r.num_columns == num_cols)
                    .collect();
                let col_avg = col_results.iter().map(|r| r.median_ms).sum::<f64>() / col_results.len() as f64;

                let overhead = col_avg - base_avg;
                let ratio = col_avg / base_avg;
                let overhead_per_col = overhead / (num_cols - base_cols) as f64;

                writeln!(file, "  {} cols: {:.2}ms", num_cols, col_avg)?;
                writeln!(file, "    Overhead: +{:.2}ms ({:.1}x)", overhead, ratio)?;
                writeln!(file, "    Per extra column: +{:.3}ms", overhead_per_col)?;
                writeln!(file)?;
            }
        }
        writeln!(file)?;
    }

    Ok(())
}
