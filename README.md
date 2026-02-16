# ChunkDB

> **Experimental prototype** — APIs, storage formats, and chunking strategies are subject to change. This project explores whether multi-dimensional chunking (as used in array stores like Zarr) can be applied to tabular/relational data to improve query performance on selective workloads. Expect bugs and rough edges.
>
> **AI-assisted development**: This project was built with significant assistance from Claude (Anthropic). Implementation, benchmarks, and documentation were developed collaboratively between a human developer and an AI assistant.

A columnar storage engine built on Arrow and Parquet that physically partitions
data across four dimensions simultaneously — row buckets, column groups, hash
buckets, and range buckets — so that queries can potentially skip irrelevant
chunks without scanning the full dataset.

Inspired by the [Zarr](https://zarr.dev/) chunked array model, ChunkDB
investigates applying the same idea to tabular data: split a logical table into
a grid of small, independently addressable Parquet files, each identified by a
multi-dimensional coordinate.

## Core idea

### From Zarr to tables

Zarr stores N-dimensional arrays as a grid of chunks. A 2D array with shape
`(10000, 200)` and chunk shape `(1000, 50)` produces a 10x4 grid; reading
slice `[2000:3000, 50:100]` touches exactly one chunk.

ChunkDB attempts to apply this to tabular data. A table with 1M rows, 30
columns, 50 sensors, and hourly timestamps is split across four axes:

```
               col_group 0          col_group 1          col_group 2
             (id,ts,sensor)      (value1..value10)    (value11..value30)
            ┌───────────────┐   ┌───────────────┐   ┌───────────────┐
row_bucket 0│               │   │               │   │               │
            ├───────────────┤   ├───────────────┤   ├───────────────┤
row_bucket 1│               │   │               │   │               │
            ├───────────────┤   ├───────────────┤   ├───────────────┤
row_bucket 2│     ...       │   │     ...       │   │     ...       │
            └───────────────┘   └───────────────┘   └───────────────┘

  Within each cell, data is further split by:
    hash_bucket[0]  = xxh3(sensor_id) % 50     (one file per sensor bucket)
    range_bucket[0] = timestamp / 3600          (one file per hour)
```

A query like `SELECT value1 WHERE sensor_id = 'sensor_3' AND timestamp
BETWEEN 7200 AND 10800` would ideally read only files at the intersection of
the matching hash bucket, range bucket(s), and column group — skipping
everything else at the metadata level without I/O.

### The chunk coordinate

Every Parquet file on disk is identified by a 4D coordinate:

```
ChunkCoordinate {
    row_bucket:    u64,        // row_id / chunk_rows
    col_group:     u16,        // which column subset
    hash_buckets:  Vec<u64>,   // one per hash dimension
    range_buckets: Vec<u64>,   // one per range dimension
}
```


**Important**: while the dimensions are logically independent (each has its own
bucketing rule), they interact in practice. The actual rows in a chunk are
determined by the **intersection** of all dimensions. For example, with
`chunk_rows = 100,000` and `hash_buckets = 50`, a chunk is addressed by
(row_bucket, hash_bucket, ...) — but if a particular sensor only appears 200
times in that row bucket, the resulting Parquet file will contain only 200 rows,
not 100,000. This **small files problem** is inherent to multi-dimensional
partitioning and is a known area for future optimization (see
[Known issues](#known-issues-and-limitations)).



---

## Chunking algorithm

### 1. Row bucketing

Each row has a `__row_id` (see [Row ID strategies](#row-id-strategies)).
For numeric columns (e.g., timestamps, dates), the value is first mapped to
an ordered u64 via `i64_to_ordered_u64()` (XOR with sign bit), then bucketed:

```
row_bucket = i64_to_ordered_u64(row_id) / chunk_rows
```

This preserves the natural ordering of the original values, including negative
ones (e.g., dates before 1970-01-01). With `chunk_rows = 100_000` and a
timestamp-based row ID, rows with nearby timestamps land in the same bucket,
providing temporal locality.

### 2. Column groups (vertical partitioning)

Columns are optionally split into groups. Each group is stored in a separate
Parquet file with a shared `__row_id` column used as the join key.

```
Group 0: __row_id, id, timestamp, sensor_id
Group 1: __row_id, value1, value2, ..., value10
Group 2: __row_id, value11, value12, ..., value30
```

A query that only needs `value1` reads only Group 1 files. A query spanning
multiple groups triggers a vertical join (inner join on `__row_id`).

### 3. Hash dimensions

A hash dimension partitions rows by the hash of a column value:

```
bucket = xxh3_64(value.as_bytes()) % num_buckets
```

A filter `sensor_id = 'sensor_3'` computes
the bucket, allowing the query engine to skip non-matching chunks. Multiple
hash dimensions are supported (the coordinate stores one bucket per dimension).

### 4. Range dimensions

A range dimension partitions rows by value intervals:

```
bucket = i64_to_ordered_u64(value.div_euclid(chunk_size))
```

`div_euclid` (floor division) groups values into aligned intervals, and
`i64_to_ordered_u64` (the same XOR sign-bit mapping used by row bucketing)
converts the signed bucket index to an ordered u64. Negative values
(e.g., timestamps before epoch) land in contiguous, correctly ordered buckets.
A filter `timestamp BETWEEN 3600 AND 7200` maps to a set of overlapping
buckets, and only those chunks are read.

### On-disk layout

```
<base_path>/
├── <table_name>/
│   ├── chunks/
│   │   ├── chunk_r0_c0_h7_rg4610_v1.parquet
│   │   ├── chunk_r0_c1_h7_rg4610_v1.parquet
│   │   ├── chunk_r1_c0_h3_rg4611_v2.parquet
│   │   └── ...
│   └── catalog/    <-- sled KV store
```

Filename format: `chunk_r{row}_c{col}_h{hash0}-{hash1}_rg{range0}_v{version}.parquet`

---

## Three-layer pruning

Queries pass through three pruning stages, from coarsest to finest. The
effectiveness of each layer depends on data distribution, filter selectivity,
and chunking configuration.

### Layer 1: chunk-level pruning (no I/O)

The catalog holds all chunk coordinates. The pruner eliminates coordinates
that cannot match:

- **Hash pruning**: filter `sensor_id = X` -> compute bucket, discard
  non-matching chunks.
- **Range pruning**: filter `timestamp BETWEEN a AND b` -> enumerate
  overlapping buckets, discard the rest. One-sided filters (e.g.,
  `timestamp > X`) are bounded using global min/max statistics tracked
  by the catalog.
- **Column-group pruning**: only read groups that contain requested columns.
- **Version resolution**: keep only the latest version per coordinate, (for that transaction).

### Layer 2: row-group pruning (Parquet statistics)

Each Parquet file has per-column min/max statistics per row group. Row groups
whose statistics prove no row can match (e.g., `max < filter_value` for a
`>=` filter) are skipped without reading data.

### Layer 3: row-level filtering (Arrow SIMD)

Surviving rows are filtered with Arrow compute kernels (boolean mask, AND
across predicates). This is the only stage that examines individual values.

---

## Getting started

```rust
use chunk_db::{ChunkDb, TableBuilder, Filter};

// Open (or create) a database
let mut db = ChunkDb::open("/tmp/mydb")?;

// Define a table
let config = TableBuilder::new("events", "/tmp/mydb")
    .add_column("timestamp", "Int64", false)
    .add_column("sensor_id", "Utf8", false)
    .add_column("value", "Int64", true)
    .chunk_rows(100_000)
    .add_hash_dimension("sensor_id", 50)
    .with_primary_key_as_row_id("timestamp")
    .build();

db.create_table(config)?;

// Insert data (Arrow RecordBatch)
let batch = RecordBatch::try_from_iter(vec![
    ("timestamp", Arc::new(Int64Array::from(vec![0, 1, 2])) as _),
    ("sensor_id", Arc::new(StringArray::from(vec!["s0", "s1", "s0"])) as _),
    ("value",     Arc::new(Int64Array::from(vec![100, 200, 300])) as _),
])?;
db.insert("events", &batch)?;

// Query
let count = db.select_all("events")
    .filter(Filter::eq("sensor_id", "s0"))
    .count().await?;

let rows = db.select(&["timestamp", "value"])
    .from("events")
    .filter(Filter::between("timestamp", 0, 1))
    .execute().await?;
```

Table configurations persist in the catalog. On restart, `ChunkDb::open`
reloads all tables automatically.

---

## Row ID strategies

The row ID determines which row bucket a row lands in. Four strategies:

| Strategy | Builder method | How `__row_id` is computed |
|---|---|---|
| Snowflake (default) | `.with_snowflake_id()` | Atomic counter from catalog. Sequential, unique. |
| Single column (numeric) | `.with_primary_key_as_row_id("ts")` | Order-preserving mapping to u64 via `i64_to_ordered_u64()`. Row buckets align with data values. Negative values (e.g., dates before Unix epoch) are handled correctly. |
| Single column (string) | `.with_primary_key_as_row_id("uuid")` | `xxh3_64(string)` -- deterministic hash. |
| Composite hash | `.with_composite_key_as_row_id(vec!["a","b"])` | `xxh3_64(len(a) ++ a ++ len(b) ++ b)`. Length-prefix encoding prevents collisions. |

Using a date or timestamp column as row ID is a natural choice for
time-series data: row buckets correspond to time windows, and range queries
on the timestamp can skip entire buckets. Negative values (timestamps before
Unix epoch, 1970-01-01) are supported — the order-preserving `i64_to_ordered_u64()`
mapping (XOR with sign bit) ensures that negative values land in contiguous,
correctly ordered buckets.

---

## Query API

```rust
// Full scan
db.select_all("events").execute().await?;

// Projection
db.select(&["value"]).from("events").execute().await?;

// Filters
db.select_all("events")
    .filter(Filter::eq("sensor_id", "s0"))
    .filter(Filter::between("timestamp", 0, 3600))
    .execute().await?;

// OR conditions (disjunctive normal form)
db.select_all("events")
    .filter(
        Filter::eq("sensor_id", "s0").or(Filter::eq("sensor_id", "s1"))
    )
    .execute().await?;

// Aggregations
db.select_all("events").count().await?;          // -> i64
db.select_all("events").sum("value").await?;     // -> i64
db.select_all("events").avg("value").await?;     // -> f64
db.select_all("events").min("value").await?;     // -> Option<i64>
db.select_all("events").max("value").await?;     // -> Option<i64>

// Limit
db.select_all("events").limit(100).execute().await?;
```

### Filter constructors

| Constructor | Predicate |
|---|---|
| `Filter::eq(col, val)` | `col = val` |
| `Filter::neq(col, val)` | `col != val` |
| `Filter::gt(col, val)` | `col > val` |
| `Filter::gte(col, val)` | `col >= val` |
| `Filter::lt(col, val)` | `col < val` |
| `Filter::lte(col, val)` | `col <= val` |
| `Filter::between(col, lo, hi)` | `col >= lo AND col <= hi` (returns two filters) |

Filter values accept `i64`, `i32`, `u64`, `&str`, `String`, and `bool` via `Into<FilterValue>`.

### Column projection optimization

The query engine attempts to minimize I/O by reading only the columns required:

| Query type | Columns actually read |
|---|---|
| `COUNT(*) WHERE x=y` | Only filter columns |
| `SUM(col) WHERE x=y` | Target + filter columns |
| `SELECT a, b WHERE x=y` | Projection + filter columns |
| `SELECT *` | All columns |

On a 128-column table, `COUNT(*)` reads 1-2 columns instead of 128. The effectiveness depends on column group configuration.

---

## Architecture

### Write path

```
db.insert("events", &batch)
  -> generate __row_id (based on strategy)
  -> compute hash_bucket per row (xxh3 % N)
  -> compute range_bucket per row (div_euclid)
  -> compute row_bucket per row (row_id / chunk_rows)
  -> group rows by (row_bucket, col_group, hash_buckets, range_buckets)
  -> for each coordinate:
       if coordinate already exists in catalog:
         read existing parquet, concat new rows (merge-on-write)
       write parquet file (SNAPPY compressed)
       update catalog version
  -> flush catalog
```

### Read path

```
db.select(...).filter(...).execute()
  -> build extended projection (user columns + filter columns)
  -> prune_chunks():
       load all coordinates from catalog
       eliminate by hash, range, column group, version
  -> group surviving chunks by RowKey (row_bucket + hash + range)
  -> parallel scan (Tokio async + Rayon):
       for each RowKey:
         read parquet files (one per required column group)
         if multiple groups: vertical_join on __row_id
         apply row-level filters
  -> apply final projection (remove filter-only columns)
  -> return Vec<RecordBatch>
```

### Vertical join

When a query spans multiple column groups, batches are joined on `__row_id`:

1. Compute intersection of row IDs across all groups (inner join).
2. Filter each batch to keep only common rows.
3. Build `HashMap<row_id, index>` for O(1) reordering.
4. Reorder non-reference batches to match reference order.
5. Merge columns into a single RecordBatch.

### Parallelism

- **Tokio**: top-level async orchestration, semaphore-bounded concurrency.
- **Rayon**: parallel chunk scanning, aggregation, projection within each task.
- No nested `par_iter` to avoid thread pool exhaustion.

### Catalog

Backed by [sled](https://github.com/spacejam/sled) (embedded persistent KV store):

| Key | Value |
|---|---|
| `bincode(ChunkCoordinate)` | Latest version (u64) |
| `__next_txn_id__` | Monotonic transaction counter |
| `__next_row_id__` | Snowflake allocator |
| `__table_config__<name>` | Persisted TableConfig |
| `__range_stats__<table>_<col>` | Global min/max per range dimension |

---

## Project structure

```
src/
  api/database.rs          ChunkDb (main handle), create_table, insert, select
  config/table_config.rs   TableConfig, TableBuilder, RowIdStrategy
  catalog/
    version_catalog.rs     Sled-backed version tracking, row ID allocation
    hash_registry.rs       xxh3 hash bucketing
    range_stats.rs         Global min/max statistics per range dimension
  storage/
    chunk_coord.rs         ChunkCoordinate, ChunkInfo
    chunk_naming.rs        Filename format/parse, chunk_path()
    parquet_writer.rs      write_parquet() (SNAPPY)
  partitioning/
    row_index.rs           row_bucket = row_id / chunk_rows
    range_dim.rs           range_bucket(), overlapping_buckets()
    column_groups.rs       ColumnGroupMapper
  write/
    batch_insert.rs        BatchInserter (full write pipeline, merge-on-write)
  query/
    filter.rs              Filter, FilterOp, FilterValue, CompositeFilter (OR)
    pruning.rs             Chunk-level pruning, row-group pruning
    query_builder.rs       Fluent API (select/filter/count/sum/...)
    direct_executor.rs     Query execution, row-level filtering, parquet read
    chunk_merger.rs        RowKey grouping, vertical_join, OR deduplication
```

---

## Dependencies

| Crate | Purpose |
|---|---|
| arrow 53, parquet 53 | Columnar format, compute kernels, Parquet I/O |
| thiserror | Error type derivation |
| tokio | Async runtime |
| rayon | Data parallelism |
| sled | Embedded persistent catalog |
| xxhash-rust (xxh3) | Hash bucketing and row ID hashing |
| bincode, serde | Catalog serialization |

---

## Tests and examples

```bash
cargo test                                      # 18 tests (15 unit + 3 integration)
cargo run --example playground --release        # interactive demo
cargo run --example query_benchmark --release   # simple query benchmark

# ChunkDB vs DuckDB comparison (8 query patterns)
cargo run --release --example chunkdb_vs_duckdb_benchmark -- \
  -r 2000000 -c 50 -s 200 --hash-buckets 20 --chunk-rows 50000 \
  --column-groups -b 10 -w 3
```

See [`examples/README.md`](examples/README.md) for the full list of examples and API reference.

### Benchmark snapshot (2M rows, 50 columns, 200 sensors, column groups enabled)

| Query | ChunkDB | DuckDB-Native | DuckDB-Parquet | vs Native | vs Parquet |
|---|---|---|---|---|---|
| `WHERE sensor AND ts (1%)` | **3.63ms** | 6.63ms | 150.70ms | **1.8x** | **41.5x** |
| `WHERE ts BETWEEN (1%)` | **26.71ms** | 17.18ms | 163.22ms | -1.6x | **6.1x** |
| `WHERE sensor = X` | **96.64ms** | 15.59ms | 153.53ms | -6.2x | **1.6x** |
| `WHERE ts BETWEEN (10%)` | 259.24ms | 61.41ms | 158.75ms | -4.2x | -1.6x |
| `SELECT * (full scan)` | 2042.04ms | 245.27ms | 418.41ms | -8.3x | -4.9x |

*DuckDB-Native = DuckDB's proprietary columnar format; DuckDB-Parquet = DuckDB reading standard Parquet files.*

ChunkDB outperforms DuckDB-Parquet on selective queries (1.6x-41.5x), and beats even DuckDB's native format on the combined hash+range filter (1.8x). Full scans are slower due to the small-files overhead. These benchmarks have known gaps — they do not vary data distributions, bucket skew, or selectivity curves systematically — but they suggest the approach can be competitive on its target workload. See [`benchmarks/README.md`](benchmarks/README.md) for the full 8-query breakdown and analysis.

### Benchmarks

The `benchmarks/` directory contains automated runners with plotting and analysis:

```bash
cd benchmarks
./run_parametric.sh quick    # parametric sweep (~10 min)
./run_scaling.sh quick       # column scaling analysis (~20 min)
```

See [`benchmarks/README.md`](benchmarks/README.md) for full documentation, parameter reference, and results.

### Test coverage

| Test | Count | Covers |
|---|---|---|
| Unit tests (inline) | 15 | Range bucketing (positive, negative, cross-zero, ordering), chunk filename parse/format, range stats, i64→u64 order-preserving mapping |
| `test_comprehensive_scenarios` | 1 | Multi-batch insert, hash/row bucket pruning, all filter ops, aggregations, projections, OR conditions, limit, empty results |
| `test_column_groups` | 1 | Vertical partitioning, multi-group vertical join, column projection across groups |
| `test_negative_timestamps` | 1 | Negative primary keys, range dimension with negative values, cross-zero queries |

---


## Configuration tuning

Performance appears to be sensitive to chunking parameters. Since dimensions
interact (the actual chunk content is the intersection of all dimension
predicates), choosing parameters requires reasoning about the **combined**
effect, not each dimension in isolation. The following guidelines are based
on preliminary observations and may not generalize to all workloads.

### chunk_rows

```rust
.chunk_rows(N)  // Rows per row bucket
```

**Trade-off**: Large chunks → fewer files, slower range queries. Small chunks → more files, file opening overhead.

**Empirical rule**:
```
chunk_rows × num_hash_buckets ≈ total_rows / 10-50
```

Examples:
- 100k rows, 10 sensors: `chunk_rows = 10000` → 10 row buckets × 10 hash = 100 files ✓
- 100k rows, 100 sensors: `chunk_rows = 50000` → 2 row buckets × 100 hash = 200 files ✓
- 100k rows, 100 sensors: `chunk_rows = 1000` → 100 row buckets × 100 hash = **10,000 files** ✗ (overhead!)

File opening has ~0.1-0.5ms overhead. With 1000+ files, queries spend more time opening files than reading data.

**Starting points** (to be validated per workload):
- Small datasets (<1M rows): `chunk_rows = 50,000`
- Medium (1-10M): `chunk_rows = 100,000`
- Large (>10M): `chunk_rows = 250,000+`

### num_hash_buckets

```rust
.add_hash_dimension("sensor_id", N)
```

**Trade-off**: More buckets → better pruning granularity, more files. Fewer buckets → wider scans, fewer files.

**Empirical rule**: Match hash dimension cardinality when known:
- 50 sensors → `num_buckets = 50` (1 sensor per bucket)
- 1000 sensors → `num_buckets = 100-200` (5-10 sensors per bucket)

**File count impact**: Each hash bucket creates a separate file per (row_bucket, col_group, range_bucket). With 100 hash buckets, 10 row buckets, 3 column groups = 3,000 files.

### range_dimension vs row_bucket

When using `with_primary_key_as_row_id("timestamp")`:
- **row_bucket** already provides temporal partitioning: `row_bucket = timestamp / chunk_rows`
- **range_dimension** on timestamp creates ADDITIONAL partitioning

**Design decision**:
- **Skip range_dimension for timestamp if timestamp = row_id** (avoids file explosion)
- **Use range_dimension** when you need finer temporal granularity than chunk_rows provides

Example:
```rust
// GOOD: timestamp-based row buckets only
.with_primary_key_as_row_id("timestamp")
.chunk_rows(3600)  // 1-hour buckets

// BAD: redundant dimensions
.with_primary_key_as_row_id("timestamp")
.chunk_rows(3600)
.add_range_dimension("timestamp", 3600)  // Same bucketing twice!

// GOOD: different granularity
.with_primary_key_as_row_id("id")  // Sequential ID
.add_range_dimension("timestamp", 3600)  // Hour-based pruning
```

Row bucket pruning on the primary key column is automatic. When filtering on the primary key (e.g., `timestamp BETWEEN ...`), row buckets are pruned without needing an explicit range dimension.

### Column groups

```rust
.add_column_group(vec!["id", "timestamp", "sensor_id"])
.add_column_group(vec!["value1", "value2", ...])
```
to store separately blocks of columns.



---

## Known issues and limitations

### Open issues

1. **Small files problem** — Multi-dimensional partitioning can produce many tiny Parquet files. When a hash bucket contains few rows in a given row bucket, the resulting file may hold only tens or hundreds of rows instead of `chunk_rows`. This causes excessive file opening overhead and poor I/O utilization. Future optimization: aggregate small chunks during compaction or use adaptive bucketing.
2. **No stale file cleanup** — Old version files accumulate on disk (manual cleanup required).
3. **Hardcoded concurrency** — Semaphore limit (128) not configurable.
4. **Silent type fallback in `parse_data_type()`** — Unknown type strings fall back to Utf8 instead of returning an error.
5. **Hash bucketing limited to Utf8 and Int64** — Other column types (UInt64, Float64) silently assign bucket 0, disabling hash pruning for those types.
6. **Catalog scan on every query** — `all_chunks()` performs an O(n) scan of the sled catalog. Acceptable for moderate data sizes; needs caching at scale.



### Limitations

- **Experimental status**: This is a research prototype. APIs and storage format may change between versions. Expect undiscovered bugs — the project is under active development and has not been battle-tested in production.
- **No updates/deletes**: Append-only. Workaround: write new versions with updated data.
- **Dimension interaction**: Chunk dimensions are not independent in practice — the actual chunk size is the intersection of all dimensions, which can produce many undersized files (see issue #1 above).
- **OR conditions**: Fully implemented but may read duplicate chunks (deduplication happens post-read).
- **String filters**: Only `Eq` and `NotEq` have row-level implementations. Other operators return errors.
- **No distributed mode**: Single-node only. Catalog is local sled DB.
- **No transaction isolation**: Concurrent writes to same coordinate use merge-on-write, but reads may see partial writes.
- **Batch-in-memory reads**: Queries currently materialize all matching chunks into `Vec<RecordBatch>` in memory before returning. For large result sets this can cause high memory usage. A streaming approach (returning an async `RecordBatchStream` that yields batches lazily as chunks are read) is planned but not yet implemented.
- **Query API surface**: The current filter API (`Filter::eq`, `Filter::between`, `.or()`) covers common cases but remains low-level and verbose for complex predicates. A future goal is to simplify the API (e.g., more ergonomic compound filters, builder-style predicates) and extend operator coverage (e.g., `IN`, `LIKE`, range operators on strings). This is not urgent but would improve usability.


## Roadmap

### Phase 1 — Patch log with WAL (update/delete support)

ChunkDB is currently append-only with merge-on-write. The next step introduces a **patch log** backed by a write-ahead log (WAL) to support updates, deletes, and efficient partial mutations.

**Design**:

```
Write path (current):
  INSERT batch → group by coordinate → merge-on-write → new Parquet version

Write path (proposed):
  INSERT/UPDATE/DELETE → append to WAL → write patch entry to patch log
                                          │
                                          ├─ patch type: Insert / Update / Delete
                                          ├─ affected __row_ids
                                          ├─ new values (for update)
                                          └─ target ChunkCoordinate
```

Patches would be lightweight journal entries (not full Parquet rewrites), accumulating in memory and on disk until compaction materializes them.

**Patch application has two modes**:

1. **On-the-fly at read time (merge-on-read)**: When a `SELECT` reads a chunk, pending patches for that coordinate are applied in-flight before returning results. Deletes are filtered out, updates overwrite values, inserts are appended. This avoids write amplification and provides immediate consistency without compaction.

2. **At compaction time**: A background (or manual) compaction job reads a chunk's base Parquet file, applies all accumulated patches, writes a new clean Parquet version, and truncates the patch log. This reduces read-time overhead and reclaims space from deleted rows.

```
Read path (proposed):
  SELECT → prune_chunks()
        → for each chunk:
            read base Parquet file
            check patch log for pending patches on this coordinate
            if patches exist:
              apply deletes (filter out __row_ids)
              apply updates (overwrite columns by __row_id)
              apply inserts (append rows)
            return merged RecordBatch

Compaction:
  for each coordinate with accumulated patches:
    read base Parquet + all patches
    materialize into new Parquet version
    truncate applied patches from WAL
    update catalog version
```

**Why merge-on-read over merge-on-write for mutations**: The current merge-on-write strategy (read existing Parquet, concat, dedup, rewrite) works for bulk inserts but causes write amplification for small updates. Merge-on-read defers the cost to query time, which may be acceptable when patches are small relative to chunk size. Compaction would amortize the cost over time. The trade-offs between these approaches need further investigation.

### Phase 2 — In-memory chunk cache

Add an LRU cache for recently read and recently written chunks to avoid repeated Parquet I/O.

**Design**:

```
ChunkCache {
    cache: LruCache<ChunkCoordinate, Arc<RecordBatch>>,
    max_memory_bytes: usize,
    dirty_set: HashSet<ChunkCoordinate>,  // chunks with pending patches
}
```

- **Read path**: Before reading a Parquet file, check the cache. On hit, return the cached batch (with patches applied). On miss, read from disk and populate cache.
- **Write path**: After applying patches in-flight, cache the merged result. Mark as dirty if patches are unapplied.
- **Compaction**: After compaction, update the cache entry with the clean materialized version and clear the dirty flag.
- **Eviction**: LRU by chunk coordinate, bounded by total memory. Dirty entries are flushed (compacted) before eviction.

The cache would complement merge-on-read: frequently queried chunks with pending patches could be merged once and served from cache on subsequent reads.

### Phase 3 — SQL:2023 MDA (Multi-Dimensional Array) integration

SQL:2023 introduced the **MDA** (ISO 9075-15) standard for multi-dimensional arrays as first-class SQL types. ChunkDB's 4D chunk coordinate model maps naturally to MDA semantics.

**Alignment with ChunkDB**:

| MDA concept | ChunkDB equivalent |
|---|---|
| Array dimension | Chunk dimension (row_bucket, hash, range, col_group) |
| Array cell | Individual chunk (Parquet file at a coordinate) |
| Array slice (`A[x1:x2, y1:y2]`) | Pruned coordinate subspace |
| Cell value type | RecordBatch (columnar data) |
| `TRIM` / `EXTEND` | Compaction / chunk splitting |

**Proposed integration**:

```sql
-- MDA-style array declaration mapping to ChunkDB table
CREATE ARRAY events
  WITH DIMENSIONS (
    time_bucket   REGULAR(3600),    -- range dimension, 1-hour chunks
    sensor_hash   HASHED(50),       -- hash dimension, 50 buckets
    col_group     GROUPS(3)         -- column groups
  )
  ATTRIBUTES (
    timestamp  INT64,
    sensor_id  UTF8,
    value      FLOAT64
  );

-- MDA slice maps to chunk-level pruning
SELECT value
FROM events[time_bucket(4610:4612), sensor_hash(7)]
WHERE sensor_id = 'sensor_3';

-- Equivalent to current API:
-- db.select(&["value"]).from("events")
--   .filter(Filter::eq("sensor_id", "sensor_3"))
--   .filter(Filter::between("timestamp", 4610*3600, 4612*3600))
--   .execute().await?
```

**Why MDA may fit ChunkDB**: Traditional SQL treats tables as flat row sets. MDA recognizes that scientific/analytical data is often multi-dimensional. ChunkDB's internal model (chunk coordinates as array indices) shares structural similarities with MDA's array abstraction. An open question is whether MDA syntax can serve as a natural query language for the operations ChunkDB already supports — predicate-driven subspace selection, dimensional slicing, and array aggregation — with ISO-standard semantics.

**Implementation path**:
1. SQL parser layer (sqlparser-rs) mapping MDA syntax to ChunkDB's query builder
2. Array metadata in the catalog (dimension types, bounds, chunk sizes)
3. MDA-specific operations: `CONDENSE` (aggregation over dimensions), `COVERAGE` (spatial extent), array concatenation

### Phase 4 — Operational improvements

| Feature | Description |
|---|---|
| **Streaming reads** | Replace `Vec<RecordBatch>` with async `RecordBatchStream` to yield results lazily, reducing peak memory for large result sets |
| **Background compaction** | Async task that compacts chunks with accumulated patches, reclaims space from old versions |
| **Small chunk aggregation** | Merge undersized Parquet files into larger ones during compaction to mitigate the small files problem |
| **Stale file cleanup** | GC for old Parquet versions after compaction |
| **Configurable concurrency** | Expose semaphore limit and thread pool size as runtime parameters |
| **Bloom filters** | Per-chunk Bloom filters for high-cardinality hash dimensions |
| **Predicate pushdown** | Use Arrow's `ParquetRecordBatchStream::with_filter` for native Parquet predicate evaluation |
| **Z-order clustering** | Improve locality for multi-dimensional range queries |
| **Distributed catalog** | Replace sled with distributed KV (etcd, FoundationDB) for multi-node deployment |

---

