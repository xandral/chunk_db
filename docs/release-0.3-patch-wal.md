# Release 0.3 — WAL-backed PatchLog (first slice of Phase 1)

> Status: implemented, 2026-07-08. Delivers the durability half of Phase 1 of
> [architecture-evolution.md](architecture-evolution.md): un-compacted
> updates/deletes now survive a crash. The queryable hot buffer and the
> insert-side WAL (the other half of Phase 1) are **not** in this release —
> see §5.

---

## 1. What it fixes

Since the patch-log commit, `update_rows`/`delete_rows` lived only in a
`RwLock<HashMap>`: a crash before compaction (or a split of the affected cell)
silently lost them. This was the most serious durability gap in the engine
(review doc §4). The PatchLog is now backed by a write-ahead log: every patch
is fsynced to disk **before** it becomes visible in memory, and replayed on
`ChunkDb::open`.

## 2. Design

The WAL lives *inside* `PatchLog` — every mutation flows through it, no call
site can forget it:

- `PatchLog::with_wal(path)` (used by `ChunkDb::open`, file:
  `<base_path>/wal/patches.wal`): replays the log into memory, then
  checkpoints it (rewrite from live state) so superseded records don't
  accumulate across restarts.
- `PatchLog::new()` stays volatile — tests and explicit opt-out.
- Logged mutations: `record` (Delete = bincode ids, Update/Insert = Arrow IPC
  stream), `clear_patches` / `drain` (Clear control record),
  `clear_patches_up_to` (ClearUpTo control record). Replay re-applies them in
  append order.
- Record format: `[u32 len][u64 xxh3 checksum][payload]`. Replay stops at the
  first truncated/invalid record — a crash can only corrupt the tail, and a
  partial tail record never got its fsync acknowledged.
- Lock order: `patches` map lock → `wal` mutex. The fsync happens under the
  map lock, which serializes WAL order with in-memory order.

### Checkpointing / growth

- Auto-truncate: when a clear leaves the in-memory log empty (the normal end
  of the patch lifecycle: compaction or split materialized everything), the
  WAL is rewritten to zero atomically (temp file + fsync + rename + dir
  fsync).
- Checkpoint at open: after replay the WAL is rewritten from live state.
- `PatchLog::checkpoint()` is public for manual use.
- Between checkpoints the file grows with every patch; with compaction
  running (auto or manual) it stays bounded by the live patch volume.

### Split interaction (crash ordering)

`split_cell` re-routes leftover parent patches to the child keys **before**
appending the parent's Clear record. After the `mark_refined` barrier the
parent key is frozen, so this is race-free; for crash recovery the ordering
means a crash mid-re-route at worst replays a patch on both parent and child
keys — Update/Delete application is idempotent, and parent-key leftovers are
inert (no chunks exist at the parent cell; the compactor clears them).

## 3. Crash model after 0.3

| State | Crash consequence |
|---|---|
| Un-compacted update/delete (acked) | **Recovered** from the WAL on open ✅ (was: lost) |
| update/delete not yet acked (crash during fsync) | Not applied, not durable — never acknowledged, consistent |
| Split commit | Unchanged: atomic sled batch; orphan child files harmless |
| Inserted rows (acked `insert()`) | Unchanged: durable via Parquet write + sled flush (OS page cache caveat, §5) |
| `StreamInserter` buffered rows | **Still lost** — needs the hot-buffer WAL (§5) |

## 4. Tests

- Unit (`patch_log.rs`): WAL replay round-trip (Delete/Update/Insert),
  clear/clear-up-to replay, auto-truncate on empty, corrupt-tail recovery.
- Integration: `test_patch_wal_durability_across_reopen` — update + delete
  with **no** compaction, reopen → both visible; compact → WAL truncated to
  0; third reopen still correct.

## 5. Not in this release (rest of Phase 1)

- **Queryable hot buffer**: inserts buffered in memory, visible to queries
  via merge-on-read union, flushed through the normal write path. This is the
  HTAP-lite headline of the architecture doc §4/§5.
- **Insert-side WAL**: cheap append-then-ack for inserts (today an insert is
  durable only after the full merge-on-write completes) and durability for
  the `StreamInserter` buffer — naturally the same WAL as the hot buffer.
- **fsync policy for chunk files**: Parquet writes still ride the OS page
  cache; decide and document together with the hot-buffer flush.
- **Group-commit / batched fsync**: today every patch fsyncs individually;
  fine at ChunkDB's write rates, revisit with the insert WAL.
