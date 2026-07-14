# Release 0.4 — Queryable hot buffer, insert WAL, orphan GC (Phase 1 complete)

> Status: implemented, 2026-07-12. Completes Phase 1 of
> [architecture-evolution.md](architecture-evolution.md): together with the
> WAL-backed PatchLog of [release 0.3](release-0.3-patch-wal.md), every
> acknowledged mutation — insert (buffered), update, delete — now survives a
> crash, and buffered inserts are queryable before any Parquet is written
> (the HTAP-lite property of the architecture doc §4/§5). Also adds garbage
> collection of orphaned chunk files.

---

## 1. What it fixes

Before 0.4:

- `StreamInserter` buffered rows in a private in-memory `Vec` — invisible to
  queries and **lost on crash** until the threshold flush.
- A plain `insert()` is durable only after the full merge-on-write completes
  (read existing Parquet + concat + rewrite) — expensive for small batches.
- Superseded chunk versions and pre-split parent files accumulated on disk
  forever (review doc §4, "orphan-file GC").

## 2. Hot buffer design

The key decision: **the hot buffer is not a new component**. It reuses the
WAL-backed PatchLog from 0.3 — buffered inserts are `PatchOp::Insert` entries
under a per-table key `hot:{table}`, deliberately outside the
`patch:{table}:{level}:{bucket}` namespace, so:

- **Durability, replay, checkpoint, truncation come for free** from the 0.3
  machinery: every buffered batch is fsynced before it is acknowledged and
  replayed into the buffer on `open`.
- The compactor's per-cell walk (prefix `patch:{table}:`) never touches hot
  entries, and no cell scan ever applies them as chunk patches.
- **Snapshot isolation comes for free**: hot entries carry a tx id; queries
  read `get_patches_up_to(hot_key, snapshot_tx)`.

Row ids are assigned **at buffer time** (`BatchInserter::with_row_id_column`,
same strategy machinery as the direct path), and buffered batches are stored
in full table schema with `__row_id` first. This is what makes the flush
crash-safe (§2.2).

### 2.1 Write side

- `ChunkDb::insert_buffered(table, batch) -> tx_id` — assign row ids, fsync
  to the WAL, done. No Parquet I/O.
- `StreamInserter::write` now goes through the same call: every write is
  durable and queryable immediately; the threshold only decides when the
  buffer is **materialized**, no longer when it becomes safe/visible.
- `ChunkDb::flush_hot_buffer(table)` (and the stream threshold, and
  `compact()`) materializes: concat the buffered batches, run them through
  the normal `BatchInserter::insert` path (merge-on-write, adaptive-grid
  splits included), then `clear_patches_up_to(hot_key, snapshot)` — which
  truncates the WAL when nothing else is pending.

### 2.2 Crash ordering of the flush

Flush = `insert()` then `clear`. A crash in between replays the buffered
batches on the next open and re-inserts rows that already live in chunks —
harmless, because rows kept the `__row_id` assigned at buffer time and
merge-on-write **deduplicates by `__row_id`** (keep-last). Re-flushing the
same rows is a no-op upsert. This mirrors the idempotency argument used for
the split re-route ordering in 0.3.

### 2.3 Read side

`DirectExecutor::execute_internal` unions the hot buffer with the chunk scan:
hot batches are row-filtered with the same Arrow kernels, projected to the
scan's extended projection (keeping `__row_id`), and appended before the
final projection. Two subtleties:

- **Race with a concurrent flush**: a query can observe the new chunk
  versions *and* the not-yet-cleared hot entries. The union deduplicates by
  `__row_id` against the chunk-side results (chunk side wins).
- Composite (OR) queries work unchanged: each branch unions the buffer, and
  the existing cross-branch `union_and_deduplicate` (by `__row_id`) removes
  double-counting.

### 2.4 Updates/deletes of buffered rows

Cell patches (`patch:{table}:{level}:{bucket}`) only reach materialized rows.
`update_rows` / `delete_rows` therefore **flush the hot buffer first** — the
simple correct choice for a prototype. `compact()` does the same, preserving
the invariant "after compact, everything lives in Parquet and the WAL is
empty".

## 3. Garbage collection

`ChunkDb::collect_garbage(table, min_age) -> GcResult` deletes files under
`<table>/chunks/` that are not in `live_chunk_files()` (the catalog's latest
version per live coordinate): superseded versions and pre-split parents.

Safety rules:

- The directory is listed **before** the live set is read: anything committed
  after the listing is not in it and cannot be deleted by mistake.
- `min_age` (mtime) protects files written by an in-flight insert that are
  not yet registered in the catalog.
- It is a maintenance operation: a long-running query that snapshotted its
  chunk list before the GC can fail if its files are collected under it.
  Nothing schedules it automatically yet.

## 4. Crash model after 0.4

| State | Crash consequence |
|---|---|
| Acked `insert_buffered` / `StreamInserter::write` | **Recovered** from the WAL on open ✅ (was: lost) |
| Un-compacted update/delete (acked) | Recovered (0.3) ✅ |
| Crash between hot-buffer flush and clear | Replay re-inserts; merge-on-write dedups by `__row_id` — no duplicates ✅ |
| Acked plain `insert()` | Unchanged: durable via Parquet write + sled flush (OS page-cache caveat) |
| Split commit | Unchanged: atomic sled batch; orphan child files are GC food |

## 5. Tests

- `test_hot_buffer_visible_durable_and_flushable`: buffered rows visible to
  count/filter/sum with no flush; survive reopen via replay; flush leaves no
  duplicates and truncates the WAL to 0 bytes; third reopen intact.
- `test_stream_inserter_immediately_queryable`: streamed rows queryable below
  the threshold; update/delete reach previously buffered rows (implicit
  flush); `close()` after implicit flush is a safe no-op.
- `test_gc_removes_superseded_chunk_files`: compaction leaves a superseded
  version; high `min_age` keeps it, `min_age = 0` removes it; only
  catalog-referenced files remain; data intact across reopen.

## 6. Not in this release

- **fsync policy for chunk files / sled**: the plain `insert()` path still
  rides the OS page cache (power-failure window; process crash is fine).
- **Group-commit**: every buffered insert fsyncs individually — fine at
  prototype rates, revisit if the insert WAL becomes the main write path.
- **Scheduled GC / GC-vs-reader coordination**: `collect_garbage` is manual.
- **Auto-flush by size/time** for `insert_buffered` outside `StreamInserter`.
- Phase 3 of the architecture doc: multi-dimension splits, merge of
  undersized cells, in-file sort + bloom filters, zone maps.
