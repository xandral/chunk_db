# Release 0.2 — Adaptive Row Grid (Phase 2 of the architecture evolution)

> Status: release design, 2026-07-05. Implements Phase 2 of
> [architecture-evolution.md](architecture-evolution.md) — `level` in chunk
> coordinates + split machinery on the row dimension. Agreed deviation from the
> phased plan: **Phase 1 (WAL + hot buffer) is deferred to a later MVP release**
> by explicit decision; consequences are spelled out in §7.
>
> Historical note (2026-08-16): Phase 3 is now implemented by
> [release-0.5-adaptive-multidimensional-grid.md](release-0.5-adaptive-multidimensional-grid.md).
> This document intentionally keeps the narrower 0.2 design as released.

---

## 1. Scope

**In this release:**

- `ChunkCoordinate` gains a `level: u16` field (refinement level of the row
  dimension). Filenames encode it as `_l{level}` (omitted at level 0, so v0
  names still parse).
- **Level map** per table: the only new state — the set of row cells that have
  been split, `{(level, bucket)}`. In-memory (`RwLock<HashSet>`), snapshotted
  to the sled catalog on every split, loaded on `open`.
- **Split on overflow** in `BatchInserter`: when a merged cell file exceeds
  `max_cell_rows`, the row cell is split in half; two files are emitted instead
  of one. Recursive until all children fit. Splits apply pending patches first
  (**compact-on-split**).
- **Level-aware read path**: `RowKey`, patch keys and chunk-cache keys carry
  the level; row-bucket pruning becomes interval-overlap against the cell's
  row-id range.
- **In-memory chunk index** in `VersionCatalog`: the per-query O(n) sled scan
  (`all_chunks`) is served from an in-memory map maintained on every version
  update. sled remains the persistent source, loaded once.
- New table option `max_cell_rows: Option<u64>` — `None` (default) preserves
  exact v0 fixed-grid behavior; `Some(n)` enables adaptive splitting.
- Bugfix (pre-existing, v0): `BatchInserter` never invalidated the
  `ChunkCache` when merge-on-write rewrote a chunk, so a warm cache could
  serve stale reads after an insert. The inserter now invalidates the cache
  keys of every coordinate it rewrites.

**Not in this release (deliberate):**

- WAL + queryable hot buffer (Phase 1) — deferred by decision; the `PatchLog`
  remains non-persistent (patches lost on crash).
- Splits along hash/range dimensions (extendible hashing) — Phase 3. `level`
  refines the **row axis only**; hash and range bucketing formulas are
  unchanged.
- In-file sorting, Parquet bloom filters, merge of undersized cells — Phase 3.
- Per-cell zone maps — deferred to Phase 3 together with in-file sort (they
  add little without it; Parquet row-group min/max stats already provide
  Layer-2 pruning).
- Formula-descent *enumeration* of candidate cells. Pruning still iterates the
  populated-coordinate list, but from memory instead of sled, and with
  level-aware interval math. True grid-walk enumeration only pays off once
  hash/range dims are hierarchical too.

## 2. The math (load-bearing, keep exact)

Let `W = chunk_rows` be the level-0 cell width in row-id space (row ids are
`u64`: snowflake counters, or `i64_to_ordered_u64(pk)` for numeric PKs, or
xxh3 for string PKs).

```text
bucket_at_level(row_id, W, L) = floor(row_id · 2^L / W)        (computed in u128)
cell_row_range(b, W, L)       = [ ceil(b·W / 2^L), ceil((b+1)·W / 2^L) )   (u128, clamped to u64)
```

Properties the implementation and tests rely on:

- **v0 equivalence at level 0**: `bucket_at_level(r, W, 0) == r / W` — a table
  with `max_cell_rows = None` produces bit-identical coordinates to v0.
- **Child relation**: if `bucket_at_level(r, W, L) = b`, then
  `bucket_at_level(r, W, L+1) ∈ {2b, 2b+1}`. Cell `(L, b)` splits into exactly
  `(L+1, 2b)` and `(L+1, 2b+1)`.
- **Order preservation**: negative PKs work unchanged because all math happens
  after `i64_to_ordered_u64` (XOR sign bit), same as v0.
- **Split floor**: a cell is splittable iff its row-id range spans ≥ 2 values
  and `L < 63`. A width-1 cell can exceed `max_cell_rows` only if the same
  `__row_id` is inserted more than once, which merge-dedup already collapses.

**Routing** (row_id → leaf cell) descends the level map:

```text
L = 0; b = bucket_at_level(row_id, W, 0)
while (L, b) ∈ refined_set:  L += 1; b = bucket_at_level(row_id, W, L)
→ leaf (L, b)
```

Every step is the formula; the map only says where to stop — no tree of
pivots, no B-tree. The map contains **only split cells**, so it stays small.

**Level-map invariant**: for every coordinate in the catalog,
`(coord.level, coord.row_bucket)` is a **leaf** of the refinement tree (never
a refined/internal cell). The split commit (§4) preserves this atomically.

The recommended adaptive configuration is a **coarse base**: large
`chunk_rows` (so level 0 has few, fat cells — for hashed/random row ids this
can be a single root cell spanning the whole u64 space) plus
`max_cell_rows ≈ 50k–200k` as the actual file-size control. The README's
tuning rules become irrelevant for adaptive tables.

## 3. Level map: `src/partitioning/level_map.rs` (new)

```rust
pub struct LevelMap {
    base_width: u64,                        // = chunk_rows
    refined: RwLock<HashSet<(u16, u64)>>,   // split cells
}
```

- `route(row_id) -> (u16, u64)` — descent loop above.
- `mark_refined(level, bucket)` — called by split.
- `snapshot() -> Vec<(u16, u64)>` / `from_snapshot(...)` — persistence.
- Stored per table as `Arc<LevelMap>` in `TableState`, shared by
  `BatchInserter` and `ChunkDb::delete_rows/update_rows`.
- Persisted under sled key `__level_map__{table}` (bincode `Vec<(u16, u64)>`),
  written inside the split's atomic batch, loaded in `ChunkDb::open` /
  `create_table`.

## 4. Split machinery (write path)

Trigger: in `BatchInserter::insert`, after the existing merge-on-write of a
coordinate, if the merged batch has more rows than `max_cell_rows` and the
cell is splittable.

Procedure for row cell `(L, b)` of table `T`:

1. `snapshot_tx = catalog.current_transaction_id()`; read pending patches for
   patch key `patch:T:L:b` up to `snapshot_tx`.
2. Collect **all** catalog coordinates with `(level, row_bucket) = (L, b)` —
   every hash/range/column-group combination, not just the overflowing one.
   Splitting must be uniform across the cell or `vertical_join` (which groups
   by RowKey) breaks.
3. For each such chunk: read base Parquet back from disk (simpler and uniform
   across the cell's chunks; the extra read of the just-written file is an
   accepted MVP cost), `apply_patches` (compact-on-split — children are born
   clean), partition rows by `bucket_at_level(row_id, W, L+1)`, producing up
   to two child batches per chunk.
4. Allocate one new version; write all child files
   (`chunk_r{2b|2b+1}_l{L+1}_...`).
5. **Commit atomically in a single sled batch**: insert child version keys,
   delete parent version keys, write the updated level-map snapshot. This is
   the crash-consistency point (§7).
6. Patch log maintenance: clear parent patches ≤ `snapshot_tx`; re-route
   patches > `snapshot_tx` to the child keys (Delete ids routed per id;
   Update batches partitioned by child bucket). Production patches are only
   `Update`/`Delete` — `PatchOp::Insert` has no producer outside tests.
7. Invalidate the chunk cache for the affected row cell (parent RowKeys).
8. Recurse on any child that still exceeds the threshold (skewed halves —
   this is the AMR-style convergence from the architecture doc §3.2).

The overflow check is per **file** (coordinate), because a coordinate is the
unit of I/O; column groups replicate rows vertically so per-file row count is
the correct size proxy.

## 5. Read path changes

- `RowKey` gains `level`; chunk-cache keys (`rk:{table}:{level}:{bucket}:…`)
  and patch keys (`patch:{table}:{level}:{bucket}`) include it.
- `pruning.rs`, row-bucket pruning (SingleColumn PK case): the bucket-range
  comparison is replaced by interval overlap —
  `cell_row_range(coord.row_bucket, W, coord.level)` vs
  `[i64_to_ordered_u64(min_pk), i64_to_ordered_u64(max_pk)]`. Cells at
  different levels are pruned correctly by construction.
- Version resolution deduplicates on the full coordinate **including level**
  (parent and child are distinct coordinates; the parent is removed from the
  catalog at split commit, so both can only coexist transiently — never after
  a completed commit).
- Hash/range/column-group pruning: unchanged.
- `compaction.rs`: parses the new patch-key format and matches chunks on
  `(level, row_bucket)`.
- `all_chunks` served from the catalog's in-memory index (populated from sled
  once at open, maintained on every `update_version` / split commit).

## 6. Configuration & API

```rust
// PartitioningConfig
#[serde(default)]
pub max_cell_rows: Option<u64>,   // None = v0 fixed grid; Some(n) = adaptive

// TableBuilder
.max_cell_rows(100_000)
```

`chunk_rows` keeps its role as the level-0 base width. No other public API
changes; `QueryBuilder` surface untouched.

## 7. Crash model & known gaps (WAL deferred — consequences)

- **Split commit is atomic** at the catalog level (one sled batch: children
  added + parent removed + level map snapshot). A crash before the batch
  leaves the parent intact and, at worst, orphan child *files* not referenced
  by the catalog — harmless, GC'd by a future maintenance loop. A crash after
  the batch is fully consistent. There is no window where both parent and
  children are live.
- ~~**Patches are still volatile** (`PatchLog` is in-memory). A crash loses
  un-compacted updates/deletes — unchanged from v0, fixed by Phase 1.~~
  **Superseded by release 0.3** ([release-0.3-patch-wal.md](release-0.3-patch-wal.md)):
  the PatchLog is WAL-backed; un-compacted updates/deletes survive a crash.
- **Chunk files fsync**: unchanged from v0 (Parquet writer + OS page cache).
- Concurrency assumptions are v0's: one writer per table; the split holds no
  new global locks (the sled batch is the serialization point). Concurrent
  compaction and splitting of the same cell are mutually excluded by a shared
  per-cell `CompactionLock` (key = the cell's patch key): the compactor skips
  cells it cannot lock (retried on the next pass), and a split that finds the
  cell locked skips too — the cell stays oversized until the next insert
  touches it and retries. The compactor re-reads the cell's coordinates from
  the catalog *after* acquiring the lock, so it can never resurrect a parent
  coordinate that a committed split removed. Writers route rows and record
  patches under a single `LevelMap` routing guard (read lock);
  `mark_refined` (write lock) therefore acts as a barrier, and the split
  drains the parent's patch key atomically — no patch can slip between
  re-routing and clearing.

## 8. Breaking changes (no migration — research prototype)

1. **Catalog format**: coordinate keys are `bincode(ChunkCoordinate)`; adding
   `level` changes the encoding. Old catalogs are unreadable.
2. **TableConfig encoding**: gains `max_cell_rows` (bincode, not
   self-describing).
3. **Patch-key format**: `patch:{table}:{row_bucket}` →
   `patch:{table}:{level}:{row_bucket}` (in-memory only, no on-disk impact).
4. Filenames are backward-compatible to *parse* (missing `_l` ⇒ level 0), but
   databases written by 0.2 with splits are not readable by 0.1.

Existing on-disk databases must be re-created. No migration tooling.

## 9. Component map (files touched)

| File | Change |
|---|---|
| `src/storage/chunk_coord.rs` | `level: u16` field |
| `src/storage/chunk_naming.rs` | `_l{level}` format/parse (omitted when 0) |
| `src/partitioning/row_index.rs` | `bucket_at_level`, `cell_row_range`, `max_split_level` (u128 math) |
| `src/partitioning/level_map.rs` | **new** — LevelMap (route / mark / snapshot) |
| `src/config/table_config.rs` | `max_cell_rows` on `PartitioningConfig`; `TableBuilder::max_cell_rows` |
| `src/catalog/version_catalog.rs` | in-memory chunk index; `remove_version`; atomic `commit_split`; level-map save/load |
| `src/write/batch_insert.rs` | level-map routing; check-and-split (recursive); compact-on-split; patch re-routing; cache invalidation on rewrite |
| `src/write/compaction.rs` | new patch-key parse; match on `(level, row_bucket)` |
| `src/query/chunk_merger.rs` | `RowKey.level` |
| `src/query/direct_executor.rs` | level in patch key + cache key |
| `src/query/pruning.rs` | interval-overlap row pruning; level in version-resolution key |
| `src/api/database.rs` | `TableState.level_map`; `patch_key(table, level, bucket)`; routing in `delete_rows`/`update_rows`; wiring |

## 10. Test plan / acceptance criteria

Unit:

- `bucket_at_level` ≡ `row_id / W` at level 0; child relation `{2b, 2b+1}`;
  `cell_row_range`/`bucket_at_level` round-trip at range boundaries; u128
  paths near `u64::MAX`.
- LevelMap routing with nested refinements; snapshot round-trip.
- Filename round-trip with level > 0; **legacy v0 filename parses to level 0**.
- Split unit test: insert past threshold → parent gone from catalog, children
  present, each child file ≤ threshold, no row lost or duplicated.

Integration (`tests/integration_test.rs`):

1. **Adaptive end-to-end**: table with small `max_cell_rows`, inserts across
   several batches → multiple split generations; `count()` and filtered
   queries return exactly the inserted data.
2. **Compact-on-split**: updates + deletes recorded, then an insert triggers
   the split → children reflect patches; patch log for the parent cell empty.
3. **Reopen**: after splits, reopen the DB → level map restored, same query
   results.
4. **Fixed-grid regression**: `max_cell_rows = None` → all pre-existing
   integration tests pass unchanged (v0 behavior preserved).

Acceptance: `cargo test` fully green; a skewed-insert smoke shows every chunk
file ≤ `max_cell_rows` rows (uniform-row-count property, architecture doc
§3.2).

## 11. Follow-ups already earmarked

- Phase 1 (WAL + hot buffer) — durability half shipped in 0.3 (WAL-backed
  PatchLog); the queryable hot buffer + insert-side WAL remain.
- ~~Phase 3 — hash/range splits (extendible hashing), in-file sort + bloom and
  undersized-cell merge.~~ Shipped in
  [release 0.5](release-0.5-adaptive-multidimensional-grid.md). Orphan-file GC
  shipped in release 0.4; scheduled maintenance remains open.
- Skew benchmark (small-files before/after) — planned as the headline number
  for the README once Phase 2 lands.
