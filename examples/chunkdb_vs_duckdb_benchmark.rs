use chunk_db::{ChunkDb, TableBuilder, RecordBatch, Filter};
use arrow::array::{Int64Array, StringArray, UInt64Array};
use std::sync::Arc;
use std::hint::black_box;
use std::time::Instant;
use duckdb::Connection;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about = "Parametric SELECT query benchmark")]
struct Args {
    /// Number of rows to insert
    #[arg(short = 'r', long, default_value = "1000000")]
    num_rows: usize,

    /// Number of value columns
    #[arg(short = 'c', long, default_value = "20")]
    num_columns: usize,

    /// Rows per chunk (chunk_rows parameter)
    #[arg(long, default_value = "50000")]
    chunk_rows: u64,

    /// Number of sensors (hash dimension cardinality)
    #[arg(short = 's', long, default_value = "100")]
    num_sensors: usize,

    /// Number of hash buckets for sensor_id partitioning
    #[arg(long, default_value = "10")]
    hash_buckets: u64,

    /// Timestamp range in seconds (for range dimension)
    #[arg(long, default_value = "86400")]
  //  timestamp_range: i64,

    /// Number of benchmark runs
    #[arg(short = 'b', long, default_value = "10")]
    benchmark_runs: usize,

    /// Number of warmup runs
    #[arg(short = 'w', long, default_value = "3")]
    warmup_runs: usize,

    /// Enable column groups (3 groups)
    #[arg(long)]
    column_groups: bool,
}

struct QuerySpec {
    name: String,
    description: String,
    projection: QueryProjection,
    filter: QueryFilter,
}

#[derive(Clone)]
enum QueryProjection {
    All,
    Single(usize),
    Few(Vec<usize>),
    Half,
}

#[derive(Clone)]
enum QueryFilter {
    None,
    Sensor(usize),
    TimeSmall(i64, i64),
    TimeMedium(i64, i64),
    TimeLarge(i64, i64),
    SensorAndTimeSmall(usize, i64, i64),
    SensorAndTimeMedium(usize, i64, i64),
    SensorAndTimeLarge(usize, i64, i64),
    TwoSensors(usize, usize),
    TimeExact(i64),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("================================================================================");
    println!("  ChunkDB vs DuckDB Benchmark");
    println!("================================================================================\n");

    println!("CONFIGURATION");
    println!("-------------");
    println!("  Rows:              {}", args.num_rows);
    println!("  Value columns:     {} (value1..value{})", args.num_columns, args.num_columns);
    println!("  Sensors:           {}", args.num_sensors);
    println!("  Hash buckets:      {} (~{:.1} sensors/bucket)", args.hash_buckets, args.num_sensors as f64 / args.hash_buckets as f64);
    println!("  Chunk rows:        {}", args.chunk_rows);
 //   println!("  Timestamp range:   {} seconds", args.timestamp_range);
    println!("  Column groups:     {}", if args.column_groups { "Yes (3 groups)" } else { "No" });
    println!("  Benchmark runs:    {} (after {} warmup)", args.benchmark_runs, args.warmup_runs);
    println!();
    println!("NOTE: ChunkDB reads from partitioned Parquet files (one per chunk coordinate);");
    println!("      DuckDB-Parquet reads from a single consolidated Parquet file;");
    println!("      DuckDB-Native uses DuckDB's proprietary columnar format.");
    println!("      Each engine uses its native storage layout.");
    println!();

    print!("Setting up ChunkDB... ");
    let chunkdb_start = Instant::now();
    let chunk_db = setup_chunkdb(&args).await?;
    println!("done ({:.2}s)", chunkdb_start.elapsed().as_secs_f64());

    print!("Setting up DuckDB... ");
    let duckdb_start = Instant::now();
    let (duck_mem, duck_parquet, parquet_path) = setup_duckdb(&args)?;
    println!("done ({:.2}s)", duckdb_start.elapsed().as_secs_f64());
    println!();

    let queries = generate_queries(&args);

    println!("RUNNING {} QUERIES", queries.len());
    println!("================================================================================\n");

    let mut results = Vec::new();

    for (i, query) in queries.iter().enumerate() {
        println!("Q{}: {} - {}", i + 1, query.name, query.description);

        let result = run_query_benchmark(
            &chunk_db,
            &duck_mem,
            &duck_parquet,
            &parquet_path,
            query,
            &args,
        ).await?;

        results.push((query.name.clone(), result));
        println!();
    }

    print_summary(&results, &args);

    Ok(())
}

async fn setup_chunkdb(args: &Args) -> Result<ChunkDb, Box<dyn std::error::Error>> {
    let db_path = "/tmp/select_benchmark_chunkdb";
    std::fs::remove_dir_all(db_path).ok();

    let mut db = ChunkDb::open(db_path)?;

    let mut builder = TableBuilder::new("events", db_path)
        .add_column("id", "UInt64", false)
        .add_column("timestamp", "Int64", false)
        .add_column("sensor_id", "Utf8", false)
        .chunk_rows(args.chunk_rows)
        .add_hash_dimension("sensor_id", args.hash_buckets)
        // Skip range_dimension - optimized for hash-filtered queries
        .with_primary_key_as_row_id("timestamp");

    for i in 1..=args.num_columns {
        builder = builder.add_column(&format!("value{}", i), "Int64", true);
    }

    if args.column_groups {
        let cols_per_group = args.num_columns / 3;
        builder = builder.add_column_group(vec!["id", "timestamp", "sensor_id"]);

        let group1_strings: Vec<String> = (1..=cols_per_group)
            .map(|i| format!("value{}", i))
            .collect();
        let group1_strs: Vec<&str> = group1_strings.iter().map(|s| s.as_str()).collect();
        builder = builder.add_column_group(group1_strs);

        let group2_strings: Vec<String> = ((cols_per_group + 1)..=args.num_columns)
            .map(|i| format!("value{}", i))
            .collect();
        let group2_strs: Vec<&str> = group2_strings.iter().map(|s| s.as_str()).collect();
        builder = builder.add_column_group(group2_strs);
    }

    db.create_table(builder.build())?;

    let batch_size = 10000;
    for batch_start in (0..args.num_rows).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(args.num_rows);

        let mut cols: Vec<(String, Arc<dyn arrow::array::Array>)> = vec![
            ("id".to_string(), Arc::new(UInt64Array::from_iter_values(batch_start as u64..batch_end as u64)) as _),
            // Use disordered timestamps to simulate realistic data
            // Simple hash-based permutation: (i * large_prime) % total_rows
            ("timestamp".to_string(), Arc::new(Int64Array::from_iter_values(
                (batch_start..batch_end).map(|i| ((i as u64 * 2654435761) % args.num_rows as u64) as i64)
            )) as _),
            ("sensor_id".to_string(), Arc::new(StringArray::from(
                (batch_start..batch_end).map(|i| format!("sensor_{}", i % args.num_sensors)).collect::<Vec<_>>()
            )) as _),
        ];

        for col_idx in 1..=args.num_columns {
            cols.push((
                format!("value{}", col_idx),
                Arc::new(Int64Array::from_iter(
                    (batch_start..batch_end).map(|i| Some((i * 100 * col_idx) as i64))
                )) as _,
            ));
        }

        let batch = RecordBatch::try_from_iter(cols)?;
        db.insert("events", &batch)?;
    }

    Ok(db)
}

fn setup_duckdb(args: &Args) -> Result<(Connection, Connection, String), Box<dyn std::error::Error>> {
    let conn_mem = Connection::open_in_memory()?;

    let mut create_sql = "CREATE TABLE events (id BIGINT, timestamp BIGINT, sensor_id VARCHAR".to_string();
    for i in 1..=args.num_columns {
        create_sql.push_str(&format!(", value{} BIGINT", i));
    }
    create_sql.push_str(")");

    conn_mem.execute(&create_sql, [])?;

    let batch_size = 10000;  // Larger batch size for faster insert
    for batch_start in (0..args.num_rows).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(args.num_rows);

        let mut insert_sql = "INSERT INTO events VALUES ".to_string();
        let mut values = Vec::new();

        for i in batch_start..batch_end {
            // Use same disordered timestamp as ChunkDB
            let timestamp = ((i as u64 * 2654435761) % args.num_rows as u64) as i64;
            let sensor = format!("sensor_{}", i % args.num_sensors);

            let mut row = format!("({}, {}, '{}'", i, timestamp, sensor);
            for col_idx in 1..=args.num_columns {
                row.push_str(&format!(", {}", i * 100 * col_idx));
            }
            row.push(')');
            values.push(row);
        }

        insert_sql.push_str(&values.join(","));
        conn_mem.execute(&insert_sql, [])?;
    }

    // Export to Parquet for disk-based queries
    let parquet_path = "/tmp/select_benchmark.parquet".to_string();
    conn_mem.execute(&format!("COPY events TO '{}'", parquet_path), [])?;

    // Create separate connection for parquet queries (reused across all queries for fair comparison)
    let conn_parquet = Connection::open_in_memory()?;

    // Return in-memory connection, parquet connection, and parquet path
    Ok((conn_mem, conn_parquet, parquet_path))
}

fn generate_queries(args: &Args) -> Vec<QuerySpec> {
    // Since timestamp = row index, the range is 0 to num_rows-1
    let ts_range = args.num_rows as i64;
    let sensor_idx = args.num_sensors / 2;
    let sensor_idx2 = sensor_idx + 1;

    let ts_start = 0;
    let ts_small = ts_range / 100;     // 1% of data
    let ts_medium = ts_range / 10;      // 10% of data

    vec![
        // Filtered queries first (ChunkDB strength)
        QuerySpec {
            name: "SELECT * WHERE sensor = X".to_string(),
            description: format!("Hash pruning ({:.1}%)", 100.0 / args.num_sensors as f64),
            projection: QueryProjection::All,
            filter: QueryFilter::Sensor(sensor_idx),
        },
        QuerySpec {
            name: "SELECT value1 WHERE sensor = X".to_string(),
            description: "Hash + col projection".to_string(),
            projection: QueryProjection::Single(3),
            filter: QueryFilter::Sensor(sensor_idx),
        },
        QuerySpec {
            name: "SELECT * WHERE ts BETWEEN (1%)".to_string(),
            description: "Time filter only (row-level)".to_string(),
            projection: QueryProjection::All,
            filter: QueryFilter::TimeSmall(ts_start, ts_start + ts_small),
        },
        QuerySpec {
            name: "SELECT * WHERE ts BETWEEN (10%)".to_string(),
            description: "Time filter only (row-level)".to_string(),
            projection: QueryProjection::All,
            filter: QueryFilter::TimeMedium(ts_start, ts_start + ts_medium),
        },
        QuerySpec {
            name: "SELECT * WHERE sensor AND ts (1%)".to_string(),
            description: "Combined: hash + range".to_string(),
            projection: QueryProjection::All,
            filter: QueryFilter::SensorAndTimeSmall(sensor_idx, ts_start, ts_start + ts_small),
        },
        QuerySpec {
            name: "SELECT * WHERE sensor OR sensor".to_string(),
            description: format!("OR condition ({:.1}%)", 200.0 / args.num_sensors as f64),
            projection: QueryProjection::All,
            filter: QueryFilter::TwoSensors(sensor_idx, sensor_idx2),
        },
        // Full scans last (DuckDB strength)
        QuerySpec {
            name: "SELECT value1 (full scan)".to_string(),
            description: "Single column, no filter".to_string(),
            projection: QueryProjection::Single(3),
            filter: QueryFilter::None,
        },
        QuerySpec {
            name: "SELECT * (full scan)".to_string(),
            description: "All columns, no filter".to_string(),
            projection: QueryProjection::All,
            filter: QueryFilter::None,
        },
    ]
}

struct BenchmarkResult {
    chunk_latencies: Vec<f64>,
    duck_mem_latencies: Vec<f64>,
    duck_parquet_latencies: Vec<f64>,
}

async fn run_query_benchmark(
    chunk_db: &ChunkDb,
    duck_mem: &Connection,
    duck_parquet: &Connection,
    parquet_path: &str,
    query: &QuerySpec,
    args: &Args,
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    // De-interleaved timing: run all iterations of one engine before the next
    // to avoid CPU/OS cache warming bias between engines.
    let mut chunk_latencies = Vec::new();
    let mut duck_mem_latencies = Vec::new();
    let mut duck_parquet_latencies = Vec::new();

    let mut expected_rows = 0;

    // Phase 1: ChunkDB (warmup + measured)
    for run in 0..(args.warmup_runs + args.benchmark_runs) {
        let start = Instant::now();
        let (rows, _) = black_box(run_chunkdb_query(chunk_db, query, args).await?);
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        if run == 0 { expected_rows = rows; }
        if run >= args.warmup_runs { chunk_latencies.push(elapsed); }
    }

    // Phase 2: DuckDB native format (warmup + measured)
    for run in 0..(args.warmup_runs + args.benchmark_runs) {
        let start = Instant::now();
        let duck_rows = black_box(run_duckdb_query_mem(duck_mem, query, args)?);
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        if run >= args.warmup_runs { duck_mem_latencies.push(elapsed); }
        if run == 0 && duck_rows != expected_rows {
            eprintln!("  WARNING: Row mismatch! ChunkDB={}, DuckDB-Native={}", expected_rows, duck_rows);
        }
    }

    // Phase 3: DuckDB Parquet (warmup + measured)
    for run in 0..(args.warmup_runs + args.benchmark_runs) {
        let start = Instant::now();
        let _ = black_box(run_duckdb_query_parquet(duck_parquet, parquet_path, query, args)?);
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        if run >= args.warmup_runs { duck_parquet_latencies.push(elapsed); }
    }

    println!("  Rows: {}", expected_rows);

    let chunk_p50 = percentile(&chunk_latencies, 50);
    let duck_mem_p50 = percentile(&duck_mem_latencies, 50);
    let duck_p_p50 = percentile(&duck_parquet_latencies, 50);

    println!("                    p50        min        max");
    println!("  ChunkDB:       {:6.2}ms   {:6.2}ms   {:6.2}ms",
        chunk_p50,
        chunk_latencies.iter().cloned().fold(f64::INFINITY, f64::min),
        chunk_latencies.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
    );
    println!("  DuckDB-Natv:   {:6.2}ms   {:6.2}ms   {:6.2}ms",
        duck_mem_p50,
        duck_mem_latencies.iter().cloned().fold(f64::INFINITY, f64::min),
        duck_mem_latencies.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
    );
    println!("  DuckDB-Parq:   {:6.2}ms   {:6.2}ms   {:6.2}ms",
        duck_p_p50,
        duck_parquet_latencies.iter().cloned().fold(f64::INFINITY, f64::min),
        duck_parquet_latencies.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
    );

    // Bar chart
    println!("\n  Performance (lower is better):");
    print_bar_chart("ChunkDB", chunk_p50, chunk_p50, duck_mem_p50, duck_p_p50);
    print_bar_chart("DuckDB-Natv", duck_mem_p50, chunk_p50, duck_mem_p50, duck_p_p50);
    print_bar_chart("DuckDB-Parq", duck_p_p50, chunk_p50, duck_mem_p50, duck_p_p50);

    Ok(BenchmarkResult {
        chunk_latencies,
        duck_mem_latencies,
        duck_parquet_latencies,
    })
}

async fn run_chunkdb_query(
    db: &ChunkDb,
    query: &QuerySpec,
    args: &Args,
) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    let projection: Option<Vec<String>> = match &query.projection {
        QueryProjection::All => None,
        QueryProjection::Single(idx) => {
            let col_name = if *idx < 3 {
                vec!["id", "timestamp", "sensor_id"][*idx].to_string()
            } else {
                format!("value{}", idx - 2)
            };
            Some(vec![col_name])
        }
        QueryProjection::Few(indices) => {
            let cols: Vec<String> = indices.iter().map(|&idx| {
                if idx < 3 {
                    vec!["id", "timestamp", "sensor_id"][idx].to_string()
                } else {
                    format!("value{}", idx - 2)
                }
            }).collect();
            Some(cols)
        }
        QueryProjection::Half => {
            let mut cols = vec!["id".to_string(), "timestamp".to_string(), "sensor_id".to_string()];
            for i in 1..=(args.num_columns / 2) {
                cols.push(format!("value{}", i));
            }
            Some(cols)
        }
    };

    let mut query_builder = if let Some(ref cols) = projection {
        let col_strs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        db.select(&col_strs)
    } else {
        db.select_all("events")
    };

    query_builder = query_builder.from("events");

    query_builder = match &query.filter {
        QueryFilter::None => query_builder,
        QueryFilter::Sensor(idx) => {
            let sensor = format!("sensor_{}", idx);
            query_builder.filter(Filter::eq("sensor_id", sensor.as_str()))
        }
        QueryFilter::TimeSmall(start, end) |
        QueryFilter::TimeMedium(start, end) |
        QueryFilter::TimeLarge(start, end) => {
            query_builder.filter(Filter::between("timestamp", *start, *end))
        }
        QueryFilter::SensorAndTimeSmall(idx, start, end) |
        QueryFilter::SensorAndTimeMedium(idx, start, end) |
        QueryFilter::SensorAndTimeLarge(idx, start, end) => {
            let sensor = format!("sensor_{}", idx);
            query_builder
                .filter(Filter::eq("sensor_id", sensor.as_str()))
                .filter(Filter::between("timestamp", *start, *end))
        }
        QueryFilter::TwoSensors(idx1, idx2) => {
            let sensor1 = format!("sensor_{}", idx1);
            let sensor2 = format!("sensor_{}", idx2);
            let filter = Filter::eq("sensor_id", sensor1.as_str())
                .or(Filter::eq("sensor_id", sensor2.as_str()));
            query_builder.filter(filter)
        }
        QueryFilter::TimeExact(ts) => {
            query_builder.filter(Filter::eq("timestamp", *ts))
        }
    };

    let batches = query_builder.execute().await?;

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let cols = if batches.is_empty() { 0 } else { batches[0].num_columns() };

    Ok((rows, cols))
}

fn build_sql_query(query: &QuerySpec, args: &Args, table_expr: &str) -> String {
    let select_clause = match &query.projection {
        QueryProjection::All => "*".to_string(),
        QueryProjection::Single(idx) => {
            if *idx < 3 {
                vec!["id", "timestamp", "sensor_id"][*idx].to_string()
            } else {
                format!("value{}", idx - 2)
            }
        }
        QueryProjection::Few(indices) => {
            indices.iter().map(|&idx| {
                if idx < 3 {
                    vec!["id", "timestamp", "sensor_id"][idx].to_string()
                } else {
                    format!("value{}", idx - 2)
                }
            }).collect::<Vec<_>>().join(",")
        }
        QueryProjection::Half => {
            let mut cols = vec!["id".to_string(), "timestamp".to_string(), "sensor_id".to_string()];
            for i in 1..=(args.num_columns / 2) {
                cols.push(format!("value{}", i));
            }
            cols.join(",")
        }
    };

    let where_clause = match &query.filter {
        QueryFilter::None => String::new(),
        QueryFilter::Sensor(idx) => format!(" WHERE sensor_id = 'sensor_{}'", idx),
        QueryFilter::TimeSmall(start, end) |
        QueryFilter::TimeMedium(start, end) |
        QueryFilter::TimeLarge(start, end) => {
            format!(" WHERE timestamp BETWEEN {} AND {}", start, end)
        }
        QueryFilter::SensorAndTimeSmall(idx, start, end) |
        QueryFilter::SensorAndTimeMedium(idx, start, end) |
        QueryFilter::SensorAndTimeLarge(idx, start, end) => {
            format!(" WHERE sensor_id = 'sensor_{}' AND timestamp BETWEEN {} AND {}", idx, start, end)
        }
        QueryFilter::TwoSensors(idx1, idx2) => {
            format!(" WHERE sensor_id = 'sensor_{}' OR sensor_id = 'sensor_{}'", idx1, idx2)
        }
        QueryFilter::TimeExact(ts) => format!(" WHERE timestamp = {}", ts),
    };

    format!("SELECT {} FROM {}{}", select_clause, table_expr, where_clause)
}

/// DuckDB native format query using Arrow interface for fair comparison
fn run_duckdb_query_mem(
    conn: &Connection,
    query: &QuerySpec,
    args: &Args,
) -> Result<usize, Box<dyn std::error::Error>> {
    let sql = build_sql_query(query, args, "events");

    // Use Arrow interface for batch-based counting (same as ChunkDB)
    let mut stmt = conn.prepare(&sql)?;
    let arrow_result = stmt.query_arrow([])?;

    let row_count: usize = arrow_result
        .map(|batch| batch.num_rows())
        .sum();

    Ok(row_count)
}

/// DuckDB Parquet query - reads directly from parquet file (real disk I/O)
/// Reuses connection across queries for fair comparison (no connection overhead)
fn run_duckdb_query_parquet(
    conn: &Connection,
    parquet_path: &str,
    query: &QuerySpec,
    args: &Args,
) -> Result<usize, Box<dyn std::error::Error>> {
    let table_expr = format!("read_parquet('{}')", parquet_path);
    let sql = build_sql_query(query, args, &table_expr);

    // Use Arrow interface for batch-based counting
    let mut stmt = conn.prepare(&sql)?;
    let arrow_result = stmt.query_arrow([])?;

    let row_count: usize = arrow_result
        .map(|batch| batch.num_rows())
        .sum();

    Ok(row_count)
}

fn percentile(values: &[f64], p: usize) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let rank = (sorted.len() - 1) as f64 * p as f64 / 100.0;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        let frac = rank - lo as f64;
        sorted[lo] * (1.0 - frac) + sorted[hi] * frac
    }
}

fn print_bar_chart(label: &str, value: f64, chunk_val: f64, duck_mem_val: f64, duck_p_val: f64) {
    let max_val = chunk_val.max(duck_mem_val).max(duck_p_val);
    let bar_width = 40;
    let normalized = (value / max_val * bar_width as f64) as usize;

    let bar = if value == chunk_val && value <= duck_mem_val && value <= duck_p_val {
        "█".repeat(normalized)
    } else if value == duck_mem_val.min(duck_p_val) {
        "░".repeat(normalized)
    } else {
        "▒".repeat(normalized)
    };

    println!("    {:12} {:6.2}ms |{}", label, value, bar);
}

fn print_summary(results: &[(String, BenchmarkResult)], args: &Args) {
    println!("================================================================================");
    println!("                              SUMMARY");
    println!("================================================================================\n");

    println!("Configuration: {} rows, {} cols, {} sensors, chunk_rows={}",
        args.num_rows, args.num_columns, args.num_sensors, args.chunk_rows);
    println!();

    // Main comparison table
    println!("{:<40} {:>10} {:>10} {:>10} {:>10}", "Query", "ChunkDB", "Duck-Natv", "Duck-Parq", "vs Parq");
    println!("{}", "-".repeat(85));

    let mut chunk_vs_parquet_wins = 0;
    let mut parquet_wins = 0;
    let mut total_speedup = 0.0;
    let mut filtered_count = 0;

    for (name, result) in results {
        let chunk_p50 = percentile(&result.chunk_latencies, 50);
        let duck_mem_p50 = percentile(&result.duck_mem_latencies, 50);
        let duck_p_p50 = percentile(&result.duck_parquet_latencies, 50);

        let display_name = if name.len() > 38 {
            format!("{}...", &name[..35])
        } else {
            name.clone()
        };

        // Compare ChunkDB vs DuckDB-Parquet (fair comparison: both disk-based)
        let speedup = duck_p_p50 / chunk_p50;
        let speedup_str = if chunk_p50 < duck_p_p50 {
            chunk_vs_parquet_wins += 1;
            format!("{:>6.1}x", speedup)
        } else {
            parquet_wins += 1;
            format!("{:>5.1}x", -chunk_p50 / duck_p_p50)
        };

        // Track speedup for filtered queries (not full scans)
        if !name.contains("full scan") {
            total_speedup += speedup;
            filtered_count += 1;
        }

        println!("{:<40} {:>9.2}ms {:>9.2}ms {:>9.2}ms {:>10}",
            display_name, chunk_p50, duck_mem_p50, duck_p_p50, speedup_str
        );
    }

    println!();
    println!("┌─────────────────────────────────────────────────────────────────────────────┐");
    println!("│  ChunkDB vs DuckDB-Parquet (Fair Comparison - Both Disk-Based)             │");
    println!("├─────────────────────────────────────────────────────────────────────────────┤");
    println!("│  ChunkDB wins:     {}/{:<3}                                                   │",
             chunk_vs_parquet_wins, results.len());
    println!("│  DuckDB wins:      {}/{:<3}                                                   │",
             parquet_wins, results.len());
    if filtered_count > 0 {
        println!("│  Avg speedup on filtered queries: {:.1}x                                     │",
                 total_speedup / filtered_count as f64);
    }
    println!("└─────────────────────────────────────────────────────────────────────────────┘");
    println!();

}
