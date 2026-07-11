# Code Review — Release 0.2 (Adaptive Row Grid) + Gap Analysis

> **Fix status (2026-07-07, same day):** F1–F9 and the F10 test migration are
> **implemented and green** (`cargo test`: 57 unit + 14 integration, including
> two new column-groups × update × split/compact tests that fail without the
> F1 fix). Per-finding notes below are marked ✅ FIXED with the mechanism used.
> Release-doc §7 has been updated to describe the now-real locking discipline.
> **2026-07-08:** the patch WAL (durability half of Phase 1) shipped too —
> see [release-0.3-patch-wal.md](release-0.3-patch-wal.md) and §4/§5 below.
> Still open: hot buffer + insert WAL, GC, Phase 3, README drift.

> Status: review report, 2026-07-07. Scope: the uncommitted working-tree diff
> implementing release 0.2 (13 files, ~740 insertions, plus the new
> `src/partitioning/level_map.rs`), reviewed against
> [release-0.2-adaptive-row-grid.md](release-0.2-adaptive-row-grid.md) and
> [architecture-evolution.md](architecture-evolution.md).
> Method: 8 independent review angles → 18 deduplicated candidates → per-candidate
> verification against the actual code. `cargo test` fully green at review time
> (54 unit + 12 integration, including the two new adaptive-grid tests).

---

## 1. Verdict

The release 0.2 implementation is **solid at its core**. The load-bearing math
(`bucket_at_level`, `cell_row_range`, `cell_is_splittable` — u128, clamps,
child relation), the LevelMap routing, filename round-trip with v0
compatibility, level-aware pruning (interval overlap is correct and only ever
over-includes, never wrongly prunes), and the atomic split commit via a single
sled batch are all correct and match the design doc. Several finder claims were
refuted on verification: no same-batch misrouting (splits run after all writes
and re-partition the just-written files), no misaligned cascading splits, no
filename parse-order issue.

What the review found (all since fixed, see the per-finding ✅ notes): **1
confirmed functional bug** (column groups × updates × split/compaction), **2
realistic concurrency defects** the design doc claimed were handled but
weren't, a handful of error-path/hardening gaps, and cleanup. The deliberate
deferrals (WAL, GC, Phase 3) tracked in §4 remain open.

## 2. Findings (ranked)

### F1 — CONFIRMED · ✅ FIXED · Full-schema patches applied to column-group files break split and compaction

`split_cell` (src/write/batch_insert.rs:264-268) reads each parent chunk —
**one file per column group, containing only that group's columns** — and
calls `apply_patches` with the raw patch list. `Update` patch batches carry the
full table schema (they come from `update_rows` unchanged). In `merge_update`
(src/write/patch_apply.rs:45), `concat_batches(&base.schema(), …)` then fails
with an Arrow schema-mismatch error.

- Repro: table with `column_groups` + one `update_rows` + an insert that
  crosses `max_cell_rows` → the insert returns `Err`. Same failure in
  `Compactor::compact_all` (src/write/compaction.rs:108) — **the bug
  pre-exists in compaction** and was inherited by compact-on-split; it is not
  new logic, but 0.2 adds a second, more frequently exercised trigger.
- Why tests miss it: both new integration tests use single-column-group
  tables; `test_compaction_bakes_patches` likewise.
- Note the query path does this correctly: `direct_executor` applies patches
  **after** `vertical_join` (src/query/direct_executor.rs:375→390), on the
  full joined schema.
- **Fix**: before applying, project each `Update`/`Insert` patch batch to the
  target chunk's column set (`__row_id` + the group's columns), in one shared
  helper used by both `split_cell` and `Compactor`. `Delete` patches are
  unaffected (they only need column 0). Add an integration test: column groups
  + update + split, and column groups + update + compact.
- Related fragility (same fix site): `merge_update`/`filter_out_rows` assume
  `__row_id` is column 0 instead of looking it up by name.


> ✅ **Fixed**: `project_patches_to_schema` in patch_apply.rs projects Update/Insert patch batches onto the target chunk schema (by name, `__row_id` first); used by both `split_cell` and `Compactor::compact_all`. Covered by `test_column_groups_update_then_compact` / `..._then_split`.
### F2 — PLAUSIBLE · ✅ FIXED · Split vs. auto-compaction

Release doc §7 states “Concurrent compaction of a splitting cell is prevented
by the same CompactionLock discipline as v0” — **neither `split_cell` nor
`Compactor::compact_all` takes the CompactionLock** (verified in
src/write/batch_insert.rs, src/write/compaction.rs). The compactor snapshots
`all_chunks()` once at the start (compaction.rs:60); a concurrent split then
atomically removes the parent coordinate. The compactor, working from its
stale snapshot, rewrites the parent file and calls `update_version` — which
**re-inserts the deleted parent key** into sled and the in-memory index.
Result: parent and children both live; queries return duplicated rows. The
patch-clearing on both paths can also interleave (lost or double-applied
patches).

This is inside the supported concurrency model: `AutoCompaction` is a
background thread the system itself spawns, sharing `Arc<VersionCatalog>` /
`Arc<PatchLog>` with the writer.

- **Fix (minimal)**: make compaction's catalog write conditional — a
  `compare_and_swap`-style `update_version_if_present` that refuses to
  re-create a coordinate the split removed, plus re-check coordinate liveness
  before clearing patches. **Fix (proper)**: take the per-cell CompactionLock
  in `split_cell` (making the doc's §7 claim true) and have the compactor skip
  cells it cannot lock.


> ✅ **Fixed**: shared per-cell `CompactionLock` (RAII `try_acquire` guard, key = patch key) taken by both `split_cell` and the compactor loop; the compactor re-reads the cell's coordinates via the new `chunks_for_cell` *after* locking, so a removed parent can no longer be resurrected. A split finding the cell locked skips and retries at the next insert.
### F3 — PLAUSIBLE · ✅ FIXED · chunk_index initial-load race

`ensure_index_loaded` (src/catalog/version_catalog.rs) releases the read lock,
scans sled **without holding any lock**, then inserts the result.
`update_version` updates the in-memory index only `if let Some(table_index) =
index.get_mut(table_name)`. Interleaving: writer inserts into sled after the
scan's iterator has passed that key, finds no table entry (load not finished),
skips the in-memory update; the loader then installs the stale scan. The chunk
exists in sled but is missing from the index **until process restart**, and
`all_chunks()` — i.e. every query and every split — never sees it. Reachable
with one writer + one reader thread (the model v0 explicitly supports).

- **Fix**: hold the table's write lock across the entire re-check + sled scan
  + insert in `ensure_index_loaded`. `update_version` already takes the same
  lock after its sled insert, so it either sees the loaded entry (and updates
  it) or blocks until the load completes and its key is included in the scan.
  Cheap: the load happens once per table.


> ✅ **Fixed**: `ensure_index_loaded` now holds the write lock across the whole sled scan; `update_version`/`commit_split` write sled first and take the same lock second, so no update can fall between the scan and the conditional index maintenance.
### F4 — PLAUSIBLE · ✅ FIXED · `mark_refined` before `commit_split`

src/write/batch_insert.rs marks the cell refined in the in-memory LevelMap
*before* `commit_split`. If the sled batch fails (I/O error, disk full), the
error propagates, but the LevelMap now routes all subsequent
inserts/updates/deletes of that cell to child cells that don't exist in the
catalog, while the parent chunks still hold the rows — patches keyed at child
level are never applied to the parent-level chunk; new rows create child
coordinates alongside the live parent. Self-heals only on reopen.

- **Fix**: build the snapshot without mutating (`snapshot()` + the candidate
  cell appended), pass it to `commit_split`, and call `mark_refined` **after**
  the commit returns `Ok`.


> ✅ **Fixed**: `LevelMap::snapshot_with(cell)` builds the post-split snapshot without mutating; `mark_refined` moved after a successful `commit_split`.
### F5 — PLAUSIBLE · ✅ FIXED · Patch-loss window

Sequence in `split_cell`: … `mark_refined` → `get_patches_after(parent_key,
snapshot_tx)` → `clear_patches(parent_key)`. A writer that called
`level_map.route()` *before* `mark_refined` but `patch_log.record()` *after*
`get_patches_after` has its patch deleted by `clear_patches` without being
applied or re-routed — a silently lost update/delete. Strictly this needs a
second concurrent writer (outside the documented one-writer-per-table model),
but nothing in the API (`&self` methods) enforces that model.

- **Fix**: add an atomic `PatchLog::drain(key) -> Vec<PatchEntry>` (remove and
  return under one lock); split then partitions the drained entries itself
  (≤ `snapshot_tx` → discard, already materialized; > `snapshot_tx` →
  re-route). One atomic operation replaces the get-after + clear pair and the
  window disappears.


> ✅ **Fixed** (stronger than proposed): `PatchLog::drain(key)` removes-and-returns atomically, and writers now route + record under a single `LevelMap::routing()` read guard, making `mark_refined` (write lock) a barrier — the window is closed even for concurrent writers outside the one-writer model.
### F6 — PLAUSIBLE · ✅ FIXED · Undeserializable catalog keys

`ensure_index_loaded` wraps coordinate decoding in `if let Ok(coord) = …` and
silently skips failures. With the accepted 0.2 format break (release doc §8),
opening a 0.1 database doesn't error — **it comes up empty**: every old key
fails to decode (the new `level: u16` misaligns all subsequent fields), every
table looks like it has zero chunks, and a subsequent insert happily starts
writing v2 data next to invisible v1 files. “Breaking format, no migration”
was the agreed decision; *silent* breakage was not.

- **Fix**: fail loudly — return
  `ChunkDbError::Catalog("undecodable coordinate key — database was written by
  an incompatible version")` (or at minimum count + log skipped keys). Two
  lines, converts a confusing data-loss appearance into a clear error.


> ✅ **Fixed**: `ensure_index_loaded` returns a `Serialization` error naming the table and the incompatible-version cause instead of silently skipping keys.
### F7 — PLAUSIBLE · ✅ FIXED · LevelMap accepts unvalidated snapshots

`from_snapshot` ingests any `Vec<(u16, u64)>` from sled without validation and
`route()`'s descent loop has no bound; `bucket_at_level` only
`debug_assert!`s `level <= MAX_SPLIT_LEVEL`. A corrupted/hand-edited snapshot
containing a cell at level ≥ 63 makes `route` panic in debug and compute
garbage in release. Unreachable through normal operation (`cell_is_splittable`
caps splits), so this is hardening, not a live bug — but the invariant is
currently enforced only by one caller, not by the type.

- **Fix**: validate cells in `from_snapshot` (reject/skip level >
  `MAX_SPLIT_LEVEL`), and bound the `route` loop at `MAX_SPLIT_LEVEL`.


> ✅ **Fixed**: `from_snapshot` now returns `Result` and rejects cells at/beyond `MAX_SPLIT_LEVEL`; `route` has a defensive level cap; `mark_refined` debug-asserts the invariant.
### F8 — CONFIRMED · ✅ FIXED · Hot-path waste

- `LevelMap::route` takes a RwLock read guard **per row**: per-row loops in
  `BatchInserter::insert`, `delete_rows`, `update_rows`. Acquire the guard
  once per batch (a `route_many` or a lock-once closure).
- `partition_by_child_bucket` clones the whole bucket vector, sorts + dedups
  to find at most 2 distinct values, then rescans the vector per child to
  build each mask. Children can only be `2b` and `2b+1`: one pass building
  both masks (or reusing `patch_apply::filter_batch` for the mask-apply step,
  which it currently duplicates) does the same job.
- `split_cell` calls `all_chunks(table)` — cloning every coordinate in the
  table — to find the handful of coordinates of one `(level, bucket)` cell.
  Add a filtered accessor on the catalog index.


> ✅ **Fixed**: one `RoutingGuard` per batch instead of a lock per row (insert/update/delete paths); `partition_by_child_bucket` is single-pass (min/max + complementary masks, reusing `patch_apply::filter_batch`); `chunks_for_cell` accessor replaces the clone-all `all_chunks` in split and compaction.
### F9 — CONFIRMED · ✅ FIXED · Duplication introduced in the wiring

- The `LevelMap::from_snapshot(chunk_rows, load_level_map(…)?)` block is
  copy-pasted in `ChunkDb::open` and `create_table`; the 6-argument
  `BatchInserter::new(…)` call is copy-pasted in `inserter()` and
  `stream_inserter()`. Two small private helpers on `ChunkDb` remove both.
- In `split_cell`'s patch re-routing, the `PatchOp::Update` and
  `PatchOp::Insert` match arms are identical except the constructor — merge
  them (`PatchOp::Insert` has no production producer anyway, per release doc
  §4.6).


> ✅ **Fixed**: `build_table_state` helper shared by `open`/`create_table`; `build_inserter` shared by `inserter`/`stream_inserter`; Update/Insert re-route arms merged via `reroute_patch_batch(…, PatchOp::Update|Insert)`.
### F10 — CONFIRMED · ✅ FIXED (new tests) · New tests violate

CLAUDE.md: “use `tempfile::tempdir()` for test databases.” Both new
integration tests use hardcoded `/tmp/test_…` + `remove_dir_all` — like
**every** pre-existing test in `tests/integration_test.rs` (the file contains
zero `tempdir()` calls). Hardcoded paths collide under parallel test runs of
the same name and leak state on panic. Either migrate the file to `tempdir()`
(preferred; `tempfile` is already a dev-dependency) or amend CLAUDE.md to
describe reality.


> ✅ **Fixed** for the code under review: the two new adaptive-grid tests (and the two new F1 tests) use `tempfile::tempdir()`. The pre-existing tests in the file still use `/tmp` — migrating them is P2 backlog.
### Documentation drift found during review

- **release-0.2 doc §7** claims CompactionLock protects splitting cells —
  false today (see F2). Fix the code, or the doc, in the same change.
- README drift (limitations, project structure) already tracked in CLAUDE.md —
  still pending.

## 3. Fix plan

**P0 — correctness, fix before building on 0.2** (each ~small, independent):

1. F1: shared patch-projection helper for `split_cell` + `Compactor`; add the
   column-groups × update × split/compact integration tests. *(the one bug a
   user hits deterministically)*
2. F2: CompactionLock in `split_cell` + compactor skip-if-unlockable; make
   `update_version` from compaction refuse to resurrect removed coordinates.
3. F3: hold the write lock across `ensure_index_loaded`'s scan.

**P1 — error paths & hardening** (bundle in one PR):

4. F4: move `mark_refined` after successful `commit_split`.
5. F5: atomic `PatchLog::drain(key)`; rework split's re-route to use it.
6. F6: loud error (or counted warning) on undecodable catalog keys.
7. F7: validate snapshots in `from_snapshot`; cap `route`'s descent.

**P2 — cleanup & consistency** (opportunistic):

8. F8 hot-path items (lock-once routing; one-pass partition; filtered catalog
   accessor).
9. F9 wiring duplication; merge Update/Insert re-route arms.
10. F10 test-path convention (migrate to `tempdir()`), fix release-doc §7
    CompactionLock claim, README drift.

P0+P1 are all local, low-risk changes; nothing requires reworking the 0.2
design. The design itself came through review intact.

## 4. Gap analysis — missing implementations vs. the target architecture

Deliberate deferrals, restated here with their *current cost* so the next
release can be scoped. Source: architecture-evolution.md §7 phased plan;
release-0.2 doc §1/§7/§11.

### Phase 1 (next per plan): WAL + queryable hot buffer — *the durability release*

| Missing piece | Current cost of its absence |
|---|---|
| ~~**Persistent PatchLog / WAL for updates & deletes**~~ | ✅ **Shipped 2026-07-08** ([release-0.3-patch-wal.md](release-0.3-patch-wal.md)): the PatchLog is WAL-backed (`patch_wal.rs`, fsync-before-visible, replay on open, checkpoint + auto-truncate). Un-compacted updates/deletes survive a crash. Covered by `test_patch_wal_durability_across_reopen` + 3 unit tests (replay, clear replay, corrupt-tail). |
| **WAL for inserts + hot buffer** | Inserts are durable only after the full merge-on-write path (Parquet rewrite + sled flush) completes; there is no cheap append-then-ack. Also no "insert → instantly queryable" story: `StreamInserter`'s buffered rows are invisible to queries until flush **and are still lost on crash** (the patch WAL does not cover them). |
| **Query-time union with hot buffer, dedup by `__row_id`** | Blocked on the hot buffer existing; this is the HTAP-lite headline of the architecture doc §5. |
| **fsync policy for chunk files** | Unchanged v0 behavior — Parquet writes ride the OS page cache; a crash can lose acked-flushed data. Should be decided (and documented) together with the hot-buffer flush. |

### Maintenance loop (was "AutoCompaction", architecture doc §6)

| Missing piece | Current cost |
|---|---|
| **Orphan-file GC** | Splits leave parent `.parquet` files on disk forever (only the catalog forgets them); superseded merge-on-write versions likewise. `live_chunk_files()` (added in this diff) provides the liveness oracle — the GC sweep itself doesn't exist. Disk usage grows monotonically. |
| **Merge of undersized sibling cells** | Cells only ever split; data deletion can strand many tiny cells. Phase 3 per plan. |
| **Compaction → maintenance-loop absorption** | Split-instead-of-compaction is half-done: splits compact, but the standalone `Compactor`/`AutoCompaction` still exists with its own patch-application path — which is exactly where F1/F2 live. Worth deciding whether Phase 1 retires it. |

### Phase 3: multi-dimension adaptivity + in-file selectivity

| Missing piece | Current cost |
|---|---|
| **Hash/range dimension splits (extendible hashing)** | The small-files problem is only solved on the row axis; a skewed hash dimension still produces the v0 pathology. Row-axis-only was the agreed Phase 2 scope. |
| **In-file sort by declared index dimensions + Parquet bloom filters** | Rare-value queries inside coarse cells pay full row-group scans; this is the architecture doc §3.4 answer to losing per-value tiny files. |
| **Per-cell zone maps** | Deferred with in-file sort (little value without it). |
| **Formula-descent grid enumeration in pruning** | Pruning still iterates the populated-coordinate list (now from the in-memory index, so O(catalog) per query, not O(sled)); true grid-walk only pays off once hash/range dims are hierarchical. |

### Explicitly parked

- Workload-driven splits (qd-tree) — architecture doc §7.4, optional/research,
  intentionally not part of the pitch.
- Skew benchmark (small-files before/after) — earmarked as the README headline
  number once 0.2 lands; unblocked as soon as P0 fixes are in.

## 5. Suggested sequencing

1. ~~**P0 fixes** (F1–F3) + their tests~~ ✅ done 2026-07-07.
2. ~~**P1 hardening** (F4–F7) + P2 cleanup~~ ✅ done 2026-07-07.
3. ~~**Patch WAL** (durability half of Phase 1)~~ ✅ done 2026-07-08
   ([release-0.3-patch-wal.md](release-0.3-patch-wal.md)).
4. **Commit the working tree** (0.2 + fixes + 0.3 are all uncommitted).
5. **Skew benchmark** (README headline number for the adaptive grid) — also
   the moment to fix the README drift (limitations/project structure).
6. **Rest of Phase 1: queryable hot buffer + insert-side WAL** — fresh reads
   (HTAP-lite) + insert durability/StreamInserter coverage; decide the fate
   of the standalone Compactor and the chunk-file fsync policy here.
7. **Orphan-file GC** (can ride along with Phase 1's maintenance loop;
   `live_chunk_files()` is the oracle).
8. **Phase 3** per the architecture doc (hash/range splits, in-file sort +
   bloom, undersized-cell merge). Backlog: migrate the pre-existing
   integration tests to `tempfile::tempdir()`.
