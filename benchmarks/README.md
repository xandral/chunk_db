# ChunkDB Benchmarks

Benchmark infrastructure for evaluating ChunkDB's performance across different configurations and workloads. All benchmark runners, plotters, and analysis guides live in this directory; the Rust benchmark examples themselves are in `../examples/` (Cargo convention).

## Available Benchmarks

| Benchmark | What it tests | Runner script |
|---|---|---|
| **Parametric sweep** | 1000 combinations of `chunk_rows`, `hash_buckets`, `range_dim_size` on a 1M-row dataset | `run_parametric.sh` |
| **Column scaling** | Performance vs number of columns selected (1 to 127), with 128 columns in 4 groups | `run_scaling.sh` |
| **Column groups** | Parametric sweep on a wide (128-col, 4-group) table to measure vertical join overhead | `run_colgroups.sh` |
| **ChunkDB vs DuckDB** | Head-to-head comparison across 8 query patterns with configurable parameters | Run directly (see below) |

## Quick Start

### Parametric benchmark (recommended first run)

```bash
cd benchmarks
./run_parametric.sh quick     # ~10 min, 27 configs
./run_parametric.sh full      # ~4-8 hours, 1000 configs
```

Generates heatmaps, 3D surfaces, parameter effect plots, and a statistical report in a timestamped output directory.

### Column scaling benchmark

```bash
cd benchmarks
./run_scaling.sh quick        # ~20 min
./run_scaling.sh full         # ~8-16 hours
```

Measures how query time scales with the number of projected columns (1, 3, 7, 15, 31, 63, 127).

### ChunkDB vs DuckDB benchmark

This benchmark is run directly via `cargo run` from the project root:

```bash
# Default: 1M rows, 20 columns, 100 sensors, 10 hash buckets
cargo run --release --example chunkdb_vs_duckdb_benchmark

# Customized run
cargo run --release --example chunkdb_vs_duckdb_benchmark -- \
  -r 200000 -c 30 -s 50 --chunk-rows 5000 --hash-buckets 10 \
  --column-groups -b 5 -w 2
```

#### Parameter reference

| Flag | Long form | Default | Description |
|---|---|---|---|
| `-r` | `--num-rows` | 1000000 | Total rows to insert |
| `-c` | `--num-columns` | 20 | Number of value columns |
| `-s` | `--num-sensors` | 100 | Number of distinct sensors (hash cardinality) |
| | `--chunk-rows` | 50000 | Rows per chunk (row bucket size) |
| | `--hash-buckets` | 10 | Number of hash buckets for sensor_id |
| | `--column-groups` | off | Enable 3 column groups |
| `-b` | `--benchmark-runs` | 10 | Number of timed runs per query |
| `-w` | `--warmup-runs` | 3 | Warmup runs (not counted) |

The benchmark runs 8 query patterns (full scan, single sensor, time ranges, combined filters, OR conditions, projections) and prints a comparison table.

## File Index

```
benchmarks/
  README.md                  This file
  INTERPRETING_RESULTS.md    How to read the generated plots and reports
  run_parametric.sh          Runner for parametric_benchmark example
  run_scaling.sh             Runner for column_scaling_benchmark example
  run_colgroups.sh           Runner for parametric_benchmark_colgroups example
  plot_benchmark.py          Plotter for parametric/colgroups results
  plot_scaling.py            Plotter for column-scaling results
```

## How the Runners Work

Each `run_*.sh` script:

1. Creates a Python venv in the output directory
2. Installs `pandas`, `matplotlib`, `seaborn`, `numpy`
3. Compiles the Rust example in release mode
4. Runs the benchmark, capturing output to a log file
5. Generates plots from the CSV results
6. Writes a summary `README.txt` in the output directory

In `quick` mode the scripts patch the Rust source to reduce the parameter grid from 1000 to 27 configurations, then restore the original file on exit.

## Interpreting Results

See [INTERPRETING_RESULTS.md](INTERPRETING_RESULTS.md) for a detailed guide on reading heatmaps, 3D surfaces, parameter effect plots, and distribution histograms.

## Example Results: 2M rows, 50 columns, 200 sensors

> **Note**: These results were collected on a single machine (Linux, NVMe SSD) and should be considered indicative, not definitive. Performance varies with hardware, OS caching, and data distribution. The comparison against DuckDB is intended to provide a familiar reference point, not to claim superiority — DuckDB is a mature, production-grade engine with years of optimization, while ChunkDB is an experimental AI-assisted prototype.
>
> **Known benchmark gaps**: The current benchmarks use uniform data distributions and do not systematically vary bucket skew, selectivity curves, or data correlation patterns. They are designed to show that the approach *can* be competitive on selective, multi-dimensional queries — not to provide a comprehensive performance characterization.

Run command:

```bash
cargo run --release --example chunkdb_vs_duckdb_benchmark -- \
  --num-rows 2000000 --num-columns 50 --num-sensors 200 \
  --hash-buckets 20 --chunk-rows 50000 --column-groups \
  -b 10 -w 3
```

### Summary table

*DuckDB-Native = DuckDB's proprietary columnar format; DuckDB-Parquet = DuckDB reading standard Parquet files.*

```
Query                                       ChunkDB   Duck-Natv Duck-Parq    vs Natv   vs Parq
-----------------------------------------------------------------------------------------------
SELECT * WHERE sensor = X                    96.64ms     15.59ms    153.53ms    -6.2x       1.6x
SELECT value1 WHERE sensor = X               30.47ms      3.25ms      7.44ms    -9.4x      -4.1x
SELECT * WHERE ts BETWEEN (1%)               26.71ms     17.18ms    163.22ms    -1.6x       6.1x
SELECT * WHERE ts BETWEEN (10%)             259.24ms     61.41ms    158.75ms    -4.2x      -1.6x
SELECT * WHERE sensor AND ts (1%)             3.63ms      6.63ms    150.70ms     1.8x      41.5x
SELECT * WHERE sensor OR sensor             193.96ms     13.73ms    161.02ms   -14.1x      -1.2x
SELECT value1 (full scan)                    73.68ms      4.61ms      8.66ms   -16.0x      -8.5x
SELECT * (full scan)                       2042.04ms    245.27ms    418.41ms    -8.3x      -4.9x
```

### Observations

- **Combined filter (hash + range, 1% selectivity)**: ChunkDB 3.63ms vs DuckDB-Parquet 150.70ms (**41.5x**) vs DuckDB-Native 6.63ms (**1.8x**). This is the scenario where multi-dimensional pruning has the largest effect — the query touches only a handful of chunks out of hundreds. ChunkDB is faster than DuckDB's native format on this pattern, suggesting that chunk-level pruning can outperform brute-force columnar scans even against a proprietary columnar representation.
- **Time range (1% selectivity)**: ChunkDB 26.71ms vs DuckDB-Parquet 163.22ms (**6.1x**). Row-bucket pruning appears effective here, though the gap narrows at 10% selectivity (259ms vs 158ms, **-1.6x**).
- **Hash filter (SELECT *)**: ChunkDB 96.64ms vs DuckDB-Parquet 153.53ms (**1.6x**). Hash bucketing avoids reading 19/20 partitions, but materializing all 50 columns still dominates. DuckDB-Native (15.59ms) is significantly faster, indicating that file I/O — not computation — is ChunkDB's bottleneck here.
- **Column projection (1 col, hash filter)**: ChunkDB 30.47ms vs DuckDB-Parquet 7.44ms (**-4.1x**). Despite column groups reducing I/O, DuckDB's Parquet reader is more efficient for single-column reads from a single file.
- **Full scans**: DuckDB is substantially faster (4.9x on SELECT *, 8.5x on single-column scan). ChunkDB's many-small-files layout incurs per-file opening overhead that penalizes sequential access patterns. This is a known structural limitation (see main README, "small files problem").
- **OR conditions**: ChunkDB 193.96ms vs DuckDB-Parquet 161.02ms (**-1.2x**). The current OR implementation executes separate scans and deduplicates post-read, which is suboptimal.

### Where does multi-dimensional pruning help?

The results suggest that ChunkDB's approach may be most effective for **highly selective queries** that combine multiple dimensions (hash + range + row bucket), allowing the engine to skip most chunks at the metadata level. The 41.5x ratio on the combined filter query illustrates this: when pruning eliminates >99% of I/O, the per-file overhead becomes negligible relative to the data avoided. Notably, ChunkDB beats DuckDB's native format on this pattern (3.63ms vs 6.63ms), suggesting that intelligent data layout can compensate for the overhead of a proprietary columnar representation.

### Where does it fall short?

For **full scans** and **low-selectivity queries**, the many-small-files layout appears to be a significant disadvantage. DuckDB's sequential scan path, vectorized execution, and optimized representation are better suited to these workloads. The gap between DuckDB-Native and DuckDB-Parquet on full scans (245ms vs 418ms) shows that even DuckDB pays a price for Parquet I/O — ChunkDB pays a larger one because it must open many files instead of one. This raises questions about whether adaptive chunk sizing or background compaction could mitigate the overhead.
