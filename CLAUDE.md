# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

ChunkDB is an experimental Rust storage engine (research prototype, not production) that applies Zarr-style multi-dimensional chunking to tabular data. A table is physically split into many small Parquet files, each addressed by a 4D coordinate, so selective queries can skip chunks at the metadata level.

## Commands

```bash
cargo build
cargo test                          # all tests (unit + integration)
cargo test <test_name>              # single test by name substring
cargo test --test integration_test  # integration tests only
cargo run --example playground --release   # interactive demo
```

Note: dev-dependencies include `duckdb` with the `bundled` feature — the first `cargo test`/example build compiles DuckDB from source and is very slow. Plain `cargo build` (library only) is fast.

Benchmarks (release builds, take minutes):

```bash
cargo run --release --example chunkdb_vs_duckdb_benchmark -- -r 2000000 -c 50 -s 200 \
  --hash-buckets 20 --chunk-rows 50000 --column-groups -b 10 -w 3
cd benchmarks && ./run_parametric.sh quick   # parameter sweep, plots via plot_benchmark.py
```

## Architecture

### The core model: 4D chunk coordinates + adaptive row grid

Every Parquet file on disk is addressed by a `ChunkCoordinate` (src/storage/chunk_coord.rs): `(row_bucket, level, col_group, hash_buckets[], range_buckets[])`, encoded in the filename (`chunk_r0_l2_c1_h7_rg4610_v2.parquet`, `_l` omitted at level 0; see src/storage/chunk_naming.rs). Bucketing is formula-based:

- `row_bucket = bucket_at_level(row_id, chunk_rows, level) = floor(row_id·2^level / chunk_rows)` (u128 math, src/partitioning/row_index.rs) — at level 0 this is `row_id / chunk_rows` (v0). Row ID comes from a configurable strategy (snowflake counter, numeric primary key, hashed string key, composite hash; src/config/table_config.rs). Every batch gets a `__row_id` column; it is also the join key across column groups.
- `hash_bucket = xxh3(value) % num_buckets` per hash dimension.
- `range_bucket = i64_to_ordered_u64(value.div_euclid(chunk_size))` per range dimension. `i64_to_ordered_u64` (XOR sign bit) preserves ordering for negative values — this mapping is load-bearing throughout row/range bucketing.

**Adaptive row grid** (release 0.2, docs/release-0.2-adaptive-row-grid.md): with `max_cell_rows: Some(n)` set, a row cell whose file exceeds n rows is split in half along the row axis (`level+1`, children `2b`/`2b+1`), recursively. The only extra state is the per-table `LevelMap` (src/partitioning/level_map.rs) — the set of split cells; routing descends by formula, the map only says where to stop. It is snapshotted in the catalog and reloaded on open. `max_cell_rows: None` (default) = exact v0 fixed-grid behavior. Hash/range dims do not split yet (Phase 3).

With the fixed grid, chunk dimensions intersect: a chunk holds only rows matching *all* its bucket predicates, which causes the known "small files problem" with skewed data — the adaptive grid (coarse `chunk_rows` + `max_cell_rows`) is the fix on the row axis.

### Catalog

sled KV store at `<base_path>/catalog/` (src/catalog/version_catalog.rs) maps `bincode(coordinate) → latest version`, plus special keys: transaction counter, snowflake row-ID allocator, persisted table configs, per-table level-map snapshots, and global min/max range stats (src/catalog/range_stats.rs). Queries do an O(n) scan of all coordinates (`all_chunks()`, served by an in-memory index), then prune.

### Write path (src/write/)

- `BatchInserter` (batch_insert.rs): computes `__row_id`, groups rows by coordinate, **merge-on-write** (reads existing Parquet, concats, dedups by `__row_id`, rewrites a new version), bumps catalog version.
- **Hot buffer** (release 0.4, docs/release-0.4-hot-buffer-gc.md): buffered inserts are `PatchOp::Insert` entries under key `hot:{table}` in the WAL-backed PatchLog — fsynced before ack, immediately queryable (union in the read path), replayed on `open`. Row ids are assigned at buffer time; `flush_hot` materializes through `BatchInserter::insert`, so a crash-replayed re-flush is deduplicated by `__row_id` (idempotent). `update_rows`/`delete_rows`/`compact` flush the hot buffer first (cell patches only reach materialized rows).
- `StreamInserter` (stream_insert.rs): writes through the hot buffer (durable + queryable per `write()`), materializes at a row threshold.
- **Updates/deletes** go through `PatchLog` (patch_log.rs): journal of Insert/Update/Delete patches keyed by chunk key + tx id, applied **merge-on-read** (patch_apply.rs) at query time. Since release 0.3 the PatchLog is **WAL-backed** (patch_wal.rs, `<base_path>/wal/patches.wal`): every mutation is fsynced before it is visible, replayed on `open`, checkpointed at open, and truncated when compaction/splits materialize everything. `PatchLog::new()` is the volatile variant (tests). Update/Insert patch batches carry the full table schema; compaction and splits project them per column group (`project_patches_to_schema`) before applying.
- `Compactor` (compaction.rs): reads base Parquet + pending patches, writes a clean new version, clears patches. `AutoCompaction` (auto_compaction.rs) runs it on a background thread. A shared per-cell `CompactionLock` (src/concurrency/, key = patch key) mutually excludes compaction and cell splits; both skip cells they cannot lock.
- `ChunkCache` (src/storage/chunk_cache.rs): in-memory cache of merged batches keyed by chunk key + the tx id patches were applied up to; invalidated/refreshed on writes and compaction.

### Read path (src/query/)

Three pruning layers, coarse to fine:
1. **Chunk-level** (pruning.rs): eliminate coordinates by hash bucket, range bucket overlap, column group, and version — no I/O.
2. **Row-group**: Parquet per-column min/max statistics skip row groups.
3. **Row-level** (direct_executor.rs): Arrow compute kernels apply filters to surviving rows.

Surviving chunks are grouped by RowKey (row_bucket + hash + range); when a query spans multiple column groups, batches are inner-joined on `__row_id` (`vertical_join` in chunk_merger.rs). Filter-only columns are added to the projection during scan, then stripped. After the scan, the table's **hot buffer is unioned in** (row-filtered, deduplicated by `__row_id` against a racing flush — chunk side wins). Parallelism: tokio for orchestration (semaphore-bounded, hardcoded 128), rayon inside tasks — deliberately no nested `par_iter`.

`QueryBuilder` (query_builder.rs) is the fluent public API: `db.select(...).from(...).filter(Filter::eq(...)).execute()/count()/sum()/...`.

### Entry point

`ChunkDb` (src/api/database.rs) ties everything together: `open`, `create_table` (via `TableBuilder`), `insert`, `insert_buffered`/`flush_hot_buffer`, `delete_rows`, `update_rows`, `compact`, `collect_garbage` (orphan chunk files; `min_age` guards in-flight inserts), `stream_inserter`, `start_auto_compaction`, `select`/`select_all`. Table configs persist in the catalog and reload on `open` (which also replays the WAL).

## Design direction (important)

**docs/architecture-evolution.md is the agreed target architecture (2026-06)** — read it before proposing structural changes. Summary: the fixed grid cannot survive data skew (small-files problem is structural, not a tuning issue). The plan replaces it with a **hierarchical adaptive grid**: coarse base grid, cells split in half on row-count overflow (coordinate gains a `level` field; extendible hashing for hash dims), in-file sorting + bloom filters for rare-value selectivity, a WAL-backed queryable hot buffer for durability and fresh reads, and **split-instead-of-compaction** (splits apply pending patches and rewrite clean). The README's "Configuration tuning" section is slated for obsolescence under this plan. New work should align with the phased plan in that doc. Status: **Phases 1 and 2 are complete** — adaptive row grid (0.2, docs/release-0.2-adaptive-row-grid.md), WAL-backed PatchLog (0.3, docs/release-0.3-patch-wal.md), queryable hot buffer + insert WAL + orphan GC (0.4, docs/release-0.4-hot-buffer-gc.md). Next: skew benchmark, then Phase 3 (hash/range splits, in-file sort + bloom, undersized-cell merge). docs/review-0.2-code-review.md records the 0.2 review, its fixes, and the remaining gap analysis.

## Documentation drift warnings

- README.md was realigned 2026-07-12 (mutations, hot buffer, adaptive grid, project structure, roadmap). Its benchmark numbers still predate the adaptive grid/hot buffer.
- docs/agents.md describes a `.claude/agents/` agent ecosystem that is **not currently present** in the repo — treat it as a design/aspiration doc, not as available tooling.

## Conventions

- Tests: unit tests inline in source files (`#[cfg(test)] mod tests`), integration tests in tests/integration_test.rs; use `tempfile::tempdir()` for test databases.
- Error handling via `thiserror` (`ChunkDbError`, `crate::Result` in src/error.rs).
- Filenames/coordinates must round-trip through chunk_naming.rs parse/format — keep them in sync if either changes.
