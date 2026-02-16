# ChunkDB Examples

This directory contains examples demonstrating ChunkDB's features and performance.

## Quick Start

Run any example with:
```bash
cargo run --example <example_name> --release
```

## Examples Overview

### `playground.rs`
**Interactive demo and experimentation sandbox**

A quick-start example for trying out ChunkDB's API interactively. Good for exploring table creation, insertion, and querying.

```bash
cargo run --example playground --release
```

### `query_benchmark.rs`
**Simple configurable query benchmark**

A straightforward benchmark that measures query performance on a single configuration. Useful for quick sanity checks and comparing specific parameter choices.

```bash
cargo run --example query_benchmark --release
```

### `parametric_benchmark.rs`
**1000-configuration parametric sweep**

Systematically tests all combinations of `chunk_rows` (10 values), `hash_buckets` (10 values), and `range_dim_size` (10 values) = 1000 configurations. Outputs a CSV file for analysis with the Python plotters in `../benchmarks/`.

```bash
cargo run --example parametric_benchmark --release
```

Best run via the automated runner:
```bash
cd ../benchmarks && ./run_parametric.sh quick
```

### `column_scaling_benchmark.rs`
**Column count scaling analysis**

Measures how query performance varies with the number of columns selected (1, 3, 7, 15, 31, 63, 127) on a 128-column table with 4 column groups. Tests vertical join overhead and column projection efficiency.

```bash
cargo run --example column_scaling_benchmark --release
```

Best run via the automated runner:
```bash
cd ../benchmarks && ./run_scaling.sh quick
```

### `chunkdb_vs_duckdb_benchmark.rs`
**ChunkDB vs DuckDB comparison**

Head-to-head benchmark comparing ChunkDB against DuckDB (native format and Parquet) across 8 query patterns: full scans, single-sensor lookups, time-range queries, combined filters, OR conditions, and projections.

```bash
# Default parameters
cargo run --release --example chunkdb_vs_duckdb_benchmark

# Customized
cargo run --release --example chunkdb_vs_duckdb_benchmark -- \
  -r 200000 -c 30 -s 50 --chunk-rows 5000 --hash-buckets 10 \
  --column-groups -b 5 -w 2
```

Parameters:
- `-r` / `--num-rows`: Total rows (default: 1000000)
- `-c` / `--num-columns`: Value columns (default: 20)
- `-s` / `--num-sensors`: Sensor count / hash cardinality (default: 100)
- `--chunk-rows`: Rows per chunk (default: 50000)
- `--hash-buckets`: Hash bucket count (default: 10)
- `--column-groups`: Enable 3 column groups
- `-b` / `--benchmark-runs`: Timed runs per query (default: 10)
- `-w` / `--warmup-runs`: Warmup runs (default: 3)

## Query API Reference

### Building Queries

```rust
// Select specific columns
db.select(&["col1", "col2"])
    .from("table_name")
    .filter(Filter::eq("status", "active"))
    .execute()
    .await?

// Select all columns
db.select_all("table_name")
    .filter(Filter::gt("value", 100))
    .execute()
    .await?
```

### Filters

```rust
Filter::eq("column", value)       // =
Filter::neq("column", value)      // !=
Filter::gt("column", value)       // >
Filter::gte("column", value)      // >=
Filter::lt("column", value)       // <
Filter::lte("column", value)      // <=
Filter::between("column", lo, hi) // >= lo AND <= hi (returns Vec<Filter>)
```

### Aggregations

```rust
db.select_all("table").count().await?;          // -> Result<i64>
db.select_all("table").sum("amount").await?;     // -> Result<i64>
db.select_all("table").avg("score").await?;      // -> Result<f64>
db.select_all("table").min("value").await?;      // -> Result<Option<i64>>
db.select_all("table").max("value").await?;      // -> Result<Option<i64>>
```

`min()` and `max()` return `None` when no rows match the filters, instead of returning a fabricated value.

### Limit

```rust
db.select_all("table")
    .filter(Filter::eq("category", "A"))
    .limit(100)
    .execute()
    .await?;
```

## Performance Tips

1. **Use hash dimensions for high-cardinality equality filters**
   ```rust
   .add_hash_dimension("tenant_id", 16)
   ```

2. **Use range dimensions for time-series data**
   ```rust
   .add_range_dimension("timestamp", 3600)
   ```

3. **Project only needed columns**
   ```rust
   db.select(&["col1", "col2"])  // Faster than select_all()
   ```

4. **Use column groups for wide tables**
   ```rust
   .add_column_group(vec!["hot_columns"])
   .add_column_group(vec!["cold_columns"])
   ```

5. **Set appropriate chunk_rows**
   ```rust
   .chunk_rows(10000)  // Balance: larger = fewer files, smaller = better pruning
   ```

## Data Types Supported

- **Int8/Int16/Int32/Int64**: Signed integers
- **UInt8/UInt16/UInt32/UInt64**: Unsigned integers
- **Float32/Float64**: Floating point (also accepts `"float"` / `"double"`)
- **Utf8**: Variable-length strings (also accepts `"string"`)
- **Bool**: Boolean values (also accepts `"boolean"`)
- **Date32**: Date values (also accepts `"date"`)
- **Timestamp**: Microsecond-precision timestamps

Filter values accept `i64`, `i32`, `u64`, `&str`, `String`, and `bool`.
