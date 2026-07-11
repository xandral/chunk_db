# Architecture Evolution: from Fixed Grid to Hierarchical Adaptive Grid

> Status: design document, 2026-06. Describes why the current fixed multi-dimensional
> grid cannot survive data skew, and the agreed target architecture that replaces it
> while preserving ChunkDB's coordinate-based identity.

---

## 1. Where we are (v0: fixed grid)

ChunkDB v0 partitions a table across four axes simultaneously — row buckets,
column groups, hash buckets, range buckets. Every chunk is addressed by a
coordinate computed with a **fixed formula**, decided once at table creation:

```
row_bucket   = row_id / chunk_rows
hash_bucket  = xxh3(value) % num_buckets
range_bucket = value / chunk_size
```

This is the Zarr model applied to tables: a grid of independently addressable
Parquet files. It works — selective queries that align with the configured
dimensions beat DuckDB-on-Parquet by 1.6–41x — but it has a structural flaw.

## 2. The problem: tabular data is sparse and skewed in dimension space

Zarr's grid works because arrays are **dense**: every cell has data by
construction. A table is not an array. The rows that land in a cell are the
**intersection** of all dimension predicates, and real data is skewed along
every axis:

- A rare sensor contributes 200 rows to its (row_bucket, hash_bucket) cell →
  a 200-row Parquet file. Thousands of them ("small files problem").
- A hot day or a chatty sensor overflows its cell → one giant file.
- The grid is uniform in *value space* but wildly non-uniform in *row count* —
  and row count is what determines file size, I/O efficiency, and pruning value.

Tuning cannot fix this. The empirical rules in the README
(`chunk_rows × hash_buckets ≈ total_rows / 10–50`) are an attempt to guess, at
setup time, a granularity that depends on a data distribution that changes over
time. The fixed grid is the root cause, not the parameters.

## 3. The fix: refine the grid, don't replace it

The geographic world hit the same problem (a fixed lat/long grid has overfull
cells on Milan and empty cells on the ocean) and solved it without abandoning
coordinates: **hierarchical grids** — geohash, quadtree, S2, H3. A cell that
overflows is subdivided *locally*; cell identity remains a computable
coordinate, just with one extra parameter: the refinement **level**.

ChunkDB adopts the same move. This was preferred over a free-form kd-tree
(median pivots) because the kd-tree kills the formula-based coordinate system;
the hierarchical grid keeps it:

```
v0:      bucket = f(value) / base_width
target:  bucket = f(value) / (base_width / 2^level)        # same formula + shift
```

The chunk coordinate gains a `level` field. The only new state is a small
in-memory map `{cell → refinement}` saying where the grid is refined and along
which dimension — not an arbitrary-pivot tree to serialize.

### 3.1 Coarse base grid

The base grid (level 0) is deliberately coarse — e.g. 4 hash buckets and 1-day
row buckets instead of 50 buckets guessed upfront. Consequences:

- **No small files, by construction.** A rare sensor never gets its own cell;
  its 200 rows cohabit a ~100k-row file with every other sensor in its bucket.
  Cells only get finer where data is dense.
- The "Configuration tuning" guesswork disappears: the system discovers the
  right granularity by refining.

### 3.2 Split on overflow

Each cell has a row threshold (e.g. `max_cell_rows = 100k`). When a flush
pushes a cell past it, the cell splits in half along one dimension:

```
Level 0:  [─────────── day 2: 400k ───────────]    overflow → split
Level 1:  [──── 00–12h: 320k ────][12–24h: 80k ✓]  left still overflows → split
Level 2:  [00–06: 160k][06–12: 160k]               → split both
Level 3:  [00–03 ✓][03–06 ✓][06–09 ✓][09–12 ✓]     ~80k each, done
```

Final cells are variable in value-width but **uniform in row count** — the
exact inversion of the fixed grid. Midpoint splits are less balanced than
median splits in the worst case, but converge with repeated splitting (this is
why quadtrees work on cities; in scientific computing the technique is AMR).

For hash dimensions, splitting doubles the modulo locally — bucket `h2` under
`%4` becomes children `h2` and `h6` under `%8`. This is textbook **extendible
hashing** (per-bucket local depth).

### 3.3 Choosing the split dimension

At split time the rows are in memory (the file is being rewritten anyway), so
the choice is **measured, not guessed**:

1. If the cell is dominated by a single value of the hash dimension, a hash
   split is useless (same value → same hash → one child gets everything).
   The range/time split is forced.
2. Otherwise compute both candidate partitions (cheap in-memory counts) and
   pick the most balanced.
3. Tie-break by configured query preference (e.g. time-range-heavy workloads
   prefer time splits, keeping cells aligned with predicates).

### 3.4 Rare-value queries: solve them inside the file

The coarse grid trades away the fixed grid's one advantage: a dedicated tiny
file per rare value. The replacement lives *inside* the cell file:

| Layer | Mechanism | Solves |
|---|---|---|
| Across files | hierarchical grid, split-on-overflow | overfull cells, giant files |
| | coarse base (never fine in sparse regions) | small files |
| Inside the file | rows **sorted** by declared index dimensions at flush/split; row-group min/max stats (pruning Layer 2, already implemented) | selectivity on rare values |
| | Parquet bloom filters per file | skipping cells that don't contain the value at all |

A `WHERE sensor_id = 's_rare'` query reads one or two ~8k-row row groups out of
an 80k-row file instead of a dedicated 200-row file — a few× read
amplification in exchange for orders of magnitude fewer files. This is the
trade every modern engine makes (ClickHouse: partition coarse, `ORDER BY`
fine; Delta/Iceberg: large files + Z-order + stats).

User-declared "index dimensions" from table setup become the **in-file sort
key**, no longer physical bucket axes.

## 4. Write path: hot buffer, WAL, split-instead-of-compaction

```
insert ──► WAL append (fsync — durability)
       ──► hot buffer (in-memory memtable, queryable)
                │ threshold
                ▼ flush
       route rows per cell (formula + level map)
                ▼
       merge-on-write per cell (read base Parquet + concat + rewrite)
       cell > threshold?  → emit 2 files instead of 1 (split) + update level map
       [sort by index dims + compute zone maps at write]
```

Key properties:

- **The WAL covers inserts, updates and deletes** — it also fixes v0's
  non-persistent `PatchLog` (currently a `RwLock<HashMap>`; patches are lost on
  crash until compaction).
- **Split absorbs compaction.** Splitting applies pending patches first and
  rewrites clean (compact-on-split), so patches never survive a split and the
  separate compaction concept disappears into the maintenance loop.
- **Split is nearly free at flush time.** `BatchInserter` already does
  merge-on-write (read–concat–rewrite on every flush to an existing
  coordinate); splitting just means emitting two files instead of one when the
  threshold is crossed.

## 5. Read path

```
query ──► walk the grid with predicates (formula per level; replaces the
          O(n) sled catalog scan)
      ──► candidate cell files  (ChunkCache, key = cell id)
      ──► row-group pruning (min/max) + bloom
      ──► apply pending patches (merge-on-read, existing machinery)
      ──► UNION with filtered hot buffer rows, dedup by __row_id (highest tx wins)
```

The hot-buffer union is the lightweight HTAP story: freshly inserted rows are
queryable before any flush. Routing/pruning descends the refinement map like a
map zoom — compute the level-0 coordinate, if marked refined recompute at the
next level, repeat; every step is a formula, the map only says where to stop.

## 6. Component mapping

| Reused as-is | Transformed | Removed |
|---|---|---|
| `parquet_writer`, chunk reading, row-level filters (Layer 3) | `StreamInserter` → hot buffer (becomes queryable, WAL-backed) | fixed multi-dim `ChunkCoordinate` semantics (gains `level`, loses formula-only addressing) |
| row-group pruning (Layer 2) — becomes load-bearing for rare values | `BatchInserter` → flusher with check-and-split | `HashRegistry` as a static registry (xxh3 stays for hash strategy) |
| `vertical_join`, column groups (orthogonal: one file per cell × group) | `chunk_rows` → `max_cell_rows` / `min_cell_rows` | `pruning.rs` grid enumeration (replaced by level-aware descent) |
| `ChunkCache` (key = cell id) | `AutoCompaction` → maintenance loop (split, merge of undersized cells, GC, WAL truncation) | per-coordinate `__chunk__` version keys as the primary index (catalog shrinks to: level map snapshot + counters + configs) |
| `PatchLog` + `patch_apply` (now WAL-backed; emptied by compact-on-split) | `Filter`/`QueryBuilder` API (unchanged surface, new pruner underneath) | README "Configuration tuning" section (obsolete) |

## 7. Phased plan

> Status (2026-07-08): Phase 2 shipped as release 0.2
> ([release-0.2-adaptive-row-grid.md](release-0.2-adaptive-row-grid.md), zone
> maps deferred to Phase 3); the durability half of Phase 1 shipped as
> release 0.3 ([release-0.3-patch-wal.md](release-0.3-patch-wal.md),
> WAL-backed PatchLog). Remaining from Phase 1: queryable hot buffer +
> insert-side WAL.

1. **WAL + queryable hot buffer.** Self-contained; fixes durability (the most
   serious v0 gap) and delivers the "insert → instantly queryable" demo.
   The fixed grid stays untouched.
2. **`level` in coordinates + split machinery.** Splits on the row/time
   dimension only (degenerate case ≈ adaptive row buckets). Level-aware
   pruning replaces the catalog scan. Zone maps per cell.
3. **Multi-dimension splits** (measured split-dimension choice, extendible
   hashing for hash dims) + merge of undersized neighbor cells + in-file sort
   and bloom filters.
4. **(Optional, research)** Workload-driven splits (qd-tree style): collect
   predicate statistics and re-split in the background where pruning fails.
   Explicitly *not* part of the core pitch.

## 8. Positioning

**"Geohash for tables"** — an embedded columnar store whose chunk grid refines
itself where data gets dense. Recognizable in ten seconds by anyone who knows
geohash/H3, faithful to the original Zarr-inspired coordinate idea, and it
removes the configuration guesswork that the fixed grid required. The hot
buffer adds an honest HTAP-lite property (fresh reads) without claiming full
HTAP semantics.

What this is *not* claiming: novel mechanisms. Extendible hashing, memtables,
WALs and zone maps are textbook. The product claim is the combination — an
embedded, coordinate-addressed, self-refining columnar layout with
split-instead-of-compaction — which no current open-source embedded engine
offers in this form factor.
