# ChunkDB

> **Experimental prototype** — APIs, storage formats, and chunking strategies are subject to change. This project explores whether multi-dimensional chunking (as used in array stores like Zarr) can be applied to tabular/relational data to improve query performance on selective workloads. Expect bugs and rough edges.
>
> **AI-assisted development**: This project was built with significant assistance from Claude (Anthropic). Implementation, benchmarks, and documentation were developed collaboratively between a human developer and an AI assistant.

A columnar storage engine built on Arrow and Parquet that physically partitions
data across four dimensions — row buckets, column groups, hash buckets and
range buckets — and refines overloaded row/hash/range cells locally. Queries
can skip irrelevant rectangles from their coordinates before reading data.

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

  At base level, each cell is further split by:
    hash_bucket[0]  = xxh3(sensor_id) % 50
    range_bucket[0] = floor(timestamp / 3600)

  An overloaded cell can refine one axis locally:
    hash modulus  -> 50 * 2^local_level
    range width   -> 3600 / 2^local_level
```

A query like `SELECT value1 WHERE sensor_id = 'sensor_3' AND timestamp
BETWEEN 7200 AND 10800` would ideally read only files at the intersection of
the matching hash bucket, range bucket(s), and column group — skipping
everything else at the metadata level without I/O.

### The chunk coordinate

Every Parquet file on disk is identified by a coordinate plus the local depth
of each adaptive axis:

```
ChunkCoordinate {
    row_bucket:    u64,        // floor(row_id · 2^level / chunk_rows)
    level:         u16,        // adaptive row grid refinement level (0 = base grid)
    col_group:     u16,        // which column subset
    hash_buckets:  Vec<u64>,   // one per hash dimension
    range_buckets: Vec<u64>,   // one per range dimension
    hash_levels:   Vec<u16>,   // local depth per hash dimension
    range_levels:  Vec<u16>,   // local depth per range dimension
}
```


**Important**: while the dimensions are logically independent (each has its own
bucketing rule), they interact in practice. The actual rows in a chunk are
determined by the **intersection** of all dimensions. For example, with
`chunk_rows = 100,000` and `hash_buckets = 50`, a chunk is addressed by
(row_bucket, hash_bucket, ...) — but if a particular sensor only appears 200
times in that row bucket, the resulting Parquet file will contain only 200 rows,
not 100,000. With `.max_cell_rows(n)`, the **adaptive multidimensional grid**
can split an overflowing logical cell along row, hash or range. It measures the
next-level distributions, selects the best-balanced axis and prefers a local
hash/range split when it is close to the best row split. The shape is persisted
and routing descends it by formula after reopen.

This solves overfull cells, but not every small-file case. A level-zero grid
that is already too fine can create many underfilled cells without triggering
any split; the current merge only reverses previous local splits and cannot
coalesce arbitrary base buckets. See the measured verdict in
[docs/decisive-tests-and-verdict.md](docs/decisive-tests-and-verdict.md).



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

With `.max_cell_rows(n)`, row is one candidate split axis:
`row_bucket = floor(row_id · 2^level / chunk_rows)`, with children `2b` and
`2b+1` at `level+1`. A row split applies to the whole row leaf and copies its
local hash/range topology into both children. Without `max_cell_rows` every
axis stays at level zero. See
[docs/release-0.5-adaptive-multidimensional-grid.md](docs/release-0.5-adaptive-multidimensional-grid.md).

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
Inside an overflowing cell, a hash split doubles only that axis's local
modulus: `num_buckets * 2^hash_level` (extendible hashing).

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
buckets, and only those chunks are read. A local range split increases that
coordinate's resolution using
`floor(value * 2^range_level / chunk_size)`; pruning compares each coordinate
at its own level.

### On-disk layout

```
<base_path>/
├── <table_name>/
│   ├── chunks/
│   │   ├── chunk_r0_c0_h7_rg4610_v1.parquet
│   │   ├── chunk_r0_c1_h7_rg4610_v1.parquet
│   │   ├── chunk_r5_l2_c0_h3_hl1_rg4611_rgl2_v2.parquet
│   │   └── ...
├── catalog/             <-- sled KV store
├── wal/
│   └── patches.wal      <-- write-ahead log (updates/deletes + hot buffer)
```

Filename format:
`chunk_r{row}[_l{row-level}]_c{col}_h{buckets}[_hl{levels}]_rg{buckets}[_rgl{levels}]_v{version}.parquet`.
Level components containing only zeroes are omitted, so legacy names still
parse as an unrefined coordinate.

---

## Three-layer pruning

Queries pass through three pruning stages, from coarsest to finest. The
effectiveness of each layer depends on data distribution, filter selectivity,
and chunking configuration.

### Layer 1: chunk-level pruning (no I/O)

The catalog holds all chunk coordinates. The pruner eliminates coordinates
that cannot match:

- **Hash pruning**: filter `sensor_id = X` -> compute the bucket at each
  candidate's local level, discard non-matching chunks.
- **Range pruning**: filter `timestamp BETWEEN a AND b` -> test overlap at
  each candidate's local level, discard the rest. One-sided filters (e.g.,
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
    .chunk_rows(1_000_000)          // deliberately coarse level-zero row grid
    .add_hash_dimension("sensor_id", 4)
    .max_cell_rows(100_000)         // refine row/hash/range on overflow
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
reloads all tables automatically (and replays the WAL — see below).

### Mutations and the hot buffer

```rust
// Update rows (batch must carry __row_id + all table columns) and delete by
// __row_id. Both are journaled as patches (fsynced to the WAL, applied
// merge-on-read) — no Parquet rewrite until compaction.
db.update_rows("events", &updated_batch)?;
db.delete_rows("events", &[row_id_a, row_id_b])?;
db.compact("events")?;              // materialize patches, truncate the WAL

// Buffered insert: fsynced to the WAL and immediately queryable, but not yet
// materialized to Parquet — much cheaper than insert() for small batches.
db.insert_buffered("events", &batch)?;
db.flush_hot_buffer("events")?;     // materialize (merge-on-write)

// StreamInserter writes through the hot buffer: every write() is durable and
// queryable at once; the buffer is materialized at the row threshold.
let mut stream = db.stream_inserter("events", StreamConfig::default())?;
stream.write(&batch)?;
stream.close()?;

// Remove superseded chunk versions / pre-split parents from disk.
db.collect_garbage("events", std::time::Duration::from_secs(60))?;
```

Everything acknowledged by these calls survives a crash: patches and buffered
inserts are replayed from the WAL on `open`. See
[docs/release-0.3-patch-wal.md](docs/release-0.3-patch-wal.md) and
[docs/release-0.4-hot-buffer-gc.md](docs/release-0.4-hot-buffer-gc.md).

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
  -> compute raw hash/range values
  -> route the row leaf through LevelMap
  -> descend its local hash/range DimensionMap
  -> group rows by full CellCoordinate + column group
  -> for each coordinate:
       if coordinate already exists in catalog:
         read existing parquet, concat, dedup by __row_id (merge-on-write)
       sort by range/hash/__row_id; write bounded row groups + Bloom filters
       update catalog version
  -> flush catalog
  -> for each overflow: measure every valid axis and split recursively

db.insert_buffered / StreamInserter::write
  -> assign __row_id, fsync the batch to the WAL (hot buffer)
  -> visible to queries immediately; materialized through db.insert's path
     at flush (threshold / flush_hot_buffer / compact)

db.update_rows / db.delete_rows
  -> flush the hot buffer, route by row cell
  -> fsync a patch record to the WAL (no Parquet rewrite)
  -> applied merge-on-read at query time; materialized by compaction/splits

db.compact / db.rebalance
  -> materialize patches, then coalesce underfilled local sibling leaves
```

### Read path

```
db.select(...).filter(...).execute()
  -> build extended projection (user columns + filter columns)
  -> prune_chunks():
       load all coordinates from catalog
       eliminate by hash, range, column group, version
  -> group surviving chunks by RowKey (row + every bucket/local level)
  -> parallel scan (Tokio async + Rayon):
       for each RowKey:
         chunk cache hit? serve cached batch (+ patch delta)
         read parquet files (one per required column group)
         if multiple groups: vertical_join on __row_id
         apply pending patches up to the query snapshot (merge-on-read)
         apply row-level filters
  -> union the hot buffer (filtered, deduplicated by __row_id)
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
| level map snapshot per table | Refined row cells |
| dimension map snapshot per table | Local hash/range internal nodes and split axes |

Updates, deletes and the hot buffer live in the WAL-backed `PatchLog`
(`wal/patches.wal`): every mutation is fsynced before it is acknowledged,
replayed on `open`, checkpointed at open, and truncated once compaction or
splits materialize everything.

---

## Project structure

```
src/
  api/database.rs          ChunkDb (main handle): tables, mutations, GC, queries
  config/table_config.rs   TableConfig, TableBuilder, RowIdStrategy
  catalog/
    version_catalog.rs     Sled-backed version tracking, row ID allocation,
                           topology snapshots, atomic split/merge commits
    hash_registry.rs       Level-aware xxh3 hash bucketing
    range_stats.rs         Global min/max statistics per range dimension
  storage/
    chunk_coord.rs         CellCoordinate, adaptive ChunkCoordinate, ChunkInfo
    chunk_naming.rs        Filename format/parse, chunk_path()
    parquet_writer.rs      Sort, bounded row groups, Bloom filters, SNAPPY
    chunk_cache.rs         In-memory cache of merged chunks (per tx snapshot)
  partitioning/
    row_index.rs           bucket_at_level(), cell_row_range(), split math
    level_map.rs           Adaptive row grid: refined cells + routing guard
    dimension_map.rs       Persistent local hash/range refinement tree
    range_dim.rs           Level-aware range buckets and overlap
    column_groups.rs       ColumnGroupMapper
  concurrency/
    compaction_lock.rs     Per-cell mutual exclusion (splits vs compaction)
  write/
    batch_insert.rs        Routing, merge-on-write, split chooser, local
                           split/merge, hot buffer materialization
    stream_insert.rs       StreamInserter over the hot buffer
    patch_log.rs           PatchLog: update/delete/insert journal
    patch_wal.rs           WAL backing the PatchLog (fsync, replay, checkpoint)
    patch_apply.rs         Merge-on-read patch application + projection
    compaction.rs          Compactor: base + patches -> clean new version
    auto_compaction.rs     Background compaction task
  query/
    filter.rs              Filter, FilterOp, FilterValue, CompositeFilter (OR)
    pruning.rs             Chunk-level pruning (level-aware), row-group pruning
    query_builder.rs       Fluent API (select/filter/count/sum/...)
    direct_executor.rs     Query execution, patches, hot-buffer union, filters
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
cargo test                                      # 102 active tests
cargo run --example playground --release        # interactive demo
cargo run --example query_benchmark --release   # simple query benchmark

# ChunkDB vs DuckDB comparison (8 query patterns)
cargo run --release --features duckdb-benchmark --example chunkdb_vs_duckdb_benchmark -- \
  -r 2000000 -c 50 -s 200 --hash-buckets 20 --chunk-rows 50000 \
  --column-groups -b 10 -w 3
```

See [`examples/README.md`](examples/README.md) for the full list of examples and API reference.

### Current benchmark verdict

The Phase 3 benchmark varies uniform/Zipf data and ordered/UUID row IDs. On
500k rows, the adaptive grid reduces the uniform ordered layout from 832 files
(100% small) to 68 files (4.4% small), and an equality filter on the hash key
runs in 0.757ms versus 16.721ms for the sorted-Parquet baseline. The same
baseline remains substantially faster on time ranges and full scans.

With UUID or Zipf data, 92-100% of adaptive files can still be underfilled:
split-on-overflow cannot coalesce level-zero cells that started too small. The
project is therefore promising as a selective-query layout, but is not yet a
general-purpose HTAP engine. See the complete methodology, raw results and
go/no-go decision in
[`docs/decisive-tests-and-verdict.md`](docs/decisive-tests-and-verdict.md).

### Benchmarks

The `benchmarks/` directory contains automated runners with plotting and analysis:

```bash
cd benchmarks
./run_parametric.sh quick    # parametric sweep (~10 min)
./run_scaling.sh quick       # column scaling analysis (~20 min)
```

See [`benchmarks/README.md`](benchmarks/README.md) for full documentation, parameter reference, and results.

### Test coverage

| Area | Covers |
|---|---|
| Unit tests (inline, 74) | Row/hash/range refinement math, topology snapshots, legacy migration, filenames, patch WAL/application, cache and range stats |
| Query/write integration | Multi-batch insert, pruning, all filter ops, aggregations, projections, OR, limit, column groups + vertical join, negative timestamps |
| Mutations & concurrency | Snapshot-safe cache, updates/deletes, partition-key rejection, WAL replay, concurrent insert/compaction and GC |
| Adaptive grid | Row/hash/range split, exact level-aware pruning, reopen, column groups, sibling merge, physical sort/row groups and concurrent routing |

---


## Configuration tuning

> **Note (2026-08):** `.max_cell_rows(n)` enables adaptive split on row, hash
> and range axes. Start with a deliberately coarse base grid: the engine can
> refine an oversized cell, but it cannot yet pack unrelated underfilled base
> cells together. The empirical rules below describe fixed-grid behavior and
> should not be used as adaptive defaults.

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

**Historical fixed-grid rule**:
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

**Adaptive starting point**: use a small base fanout (often 2-8), then let
local extendible-hash splits increase resolution only in overflowing cells.
Matching bucket count to cardinality is appropriate only when dedicated small
files are intentional.

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

1. **Underfilled base cells** — row/hash/range splits now handle overflow, and
   local siblings can merge back, but unrelated level-zero cells cannot be
   packed together. A base grid that starts too fine still creates many small
   files, especially with UUID and skewed dimensions.
2. **GC is manual** — `collect_garbage()` removes superseded versions and pre-split parents, but nothing schedules it; and a long-running query that snapshotted its chunk list before a GC can fail if its files are collected under it.
3. **Hardcoded concurrency** — Semaphore limit (128) not configurable.
4. **Catalog enumeration on every query** — `all_chunks()` is served by an
   in-memory index, but pruning still starts from every live coordinate rather
   than walking only predicate-compatible leaves.
5. **Serialized writers** — a per-table lock makes insert, compaction and
   rebalance correct, but limits write concurrency.
6. **Bloom filters are write-only** — hash keys and `__row_id` receive Parquet
   Bloom filters, but the direct executor currently uses only coordinate and
   row-group-statistics pruning.
7. **Chunk files ride the OS page cache** — Parquet writes and the sled catalog are not fsynced per insert; an acknowledged `insert()` can be lost on power failure (not on process crash). `insert_buffered` (WAL-backed) is the durable path; a chunk-file fsync policy is future work.



### Limitations

- **Experimental status**: This is a research prototype. APIs and storage
  formats may change; level-zero coordinate keys have a migration path, but
  there is no general on-disk format migration framework.
- **Updates/deletes are patch-based**: `update_rows`/`delete_rows` journal patches applied merge-on-read; heavy un-compacted patch volume slows reads until compaction. Updates must carry the full table schema.
- **Dimension interaction**: Chunk size is the intersection of every axis.
  Adaptive refinement prevents oversized leaves; it cannot yet combine sparse
  base intersections into one physical segment.
- **Partition-key updates do not relocate**: an update that would change its
  hash/range cell is rejected explicitly; perform delete + insert instead.
- **OR conditions**: Fully implemented but may read duplicate chunks (deduplication happens post-read).
- **String filters**: Only `Eq` and `NotEq` have row-level implementations. Other operators return errors.
- **No distributed mode**: Single-node only. Catalog is local sled DB.
- **Snapshot isolation is patch-level only**: queries see patches up to their
  snapshot transaction, but base Parquet rewrites are represented only by the
  latest catalog version, not a complete historical MVCC chain.
- **Batch-in-memory reads**: Queries currently materialize all matching chunks into `Vec<RecordBatch>` in memory before returning. For large result sets this can cause high memory usage. A streaming approach (returning an async `RecordBatchStream` that yields batches lazily as chunks are read) is planned but not yet implemented.
- **Query API surface**: The current filter API (`Filter::eq`, `Filter::between`, `.or()`) covers common cases but remains low-level and verbose for complex predicates. A future goal is to simplify the API (e.g., more ergonomic compound filters, builder-style predicates) and extend operator coverage (e.g., `IN`, `LIKE`, range operators on strings). This is not urgent but would improve usability.


## Status and roadmap

The agreed target architecture is the **hierarchical adaptive grid**
("geohash for tables") described in
[docs/architecture-evolution.md](docs/architecture-evolution.md) — read that
before proposing structural changes. Progress against its phased plan:

| Phase | Content | Status |
|---|---|---|
| 1 | WAL-backed PatchLog (durable updates/deletes) | ✅ release 0.3 — [docs/release-0.3-patch-wal.md](docs/release-0.3-patch-wal.md) |
| 1 | Queryable hot buffer + insert-side WAL, orphan GC | ✅ release 0.4 — [docs/release-0.4-hot-buffer-gc.md](docs/release-0.4-hot-buffer-gc.md) |
| 2 | Adaptive row grid (`level` in coordinates, split-on-overflow, compact-on-split) | ✅ release 0.2 — [docs/release-0.2-adaptive-row-grid.md](docs/release-0.2-adaptive-row-grid.md) |
| 3 | Local hash/range splits, sibling merge, in-file sort, bounded row groups and Bloom writes | ✅ release 0.5 — [docs/release-0.5-adaptive-multidimensional-grid.md](docs/release-0.5-adaptive-multidimensional-grid.md) |
| Next | Logical-cell to packed-segment directory, immutable deltas and manifest MVCC | required by benchmark verdict |
| 4 | Workload-driven splits (qd-tree style) | optional / research, after packing |

Earlier releases also shipped an in-memory chunk cache and background
auto-compaction. The 0.2 code review and its gap analysis live in
[docs/review-0.2-code-review.md](docs/review-0.2-code-review.md).

Operational backlog (not phase-bound): query-time Bloom consumption,
formula-only candidate enumeration, fsync policy for chunk files, scheduled
GC, streaming reads (`RecordBatchStream`), configurable concurrency,
group-commit for the WAL and broader filter/operator coverage. A SQL:2023 MDA
(multi-dimensional array) syntax layer over the coordinate model remains an
exploratory idea.
