use std::path::{Path, PathBuf};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::catalog::VersionCatalog;
use crate::storage::{chunk_path, write_parquet, ChunkCache};
use crate::write::patch_log::PatchLog;
use crate::write::patch_apply::apply_patches;
use crate::{ChunkDbError, Result};

pub struct Compactor<'a> {
    catalog: &'a VersionCatalog,
    patch_log: &'a PatchLog,
    chunk_cache: &'a ChunkCache,
    base_path: &'a PathBuf,
}

#[derive(Debug)]
pub struct CompactionResult {
    pub chunks_compacted: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub patches_applied: usize,
}

impl CompactionResult {
    pub fn noop() -> Self {
        Self {
            chunks_compacted: 0,
            bytes_before: 0,
            bytes_after: 0,
            patches_applied: 0,
        }
    }
}

impl<'a> Compactor<'a> {
    pub fn new(
        catalog: &'a VersionCatalog,
        patch_log: &'a PatchLog,
        chunk_cache: &'a ChunkCache,
        base_path: &'a PathBuf,
    ) -> Self {
        Self { catalog, patch_log, chunk_cache, base_path }
    }

    /// Compact all dirty row_buckets for a table.
    /// Takes a snapshot tx_id, applies only patches up to that tx_id,
    /// and clears only those patches (preserving newer ones for concurrent readers).
    pub fn compact_all(&self, table_name: &str) -> Result<CompactionResult> {
        // Take a compaction snapshot — only apply patches up to this tx_id
        let compact_tx = self.catalog.current_transaction_id();

        let dirty_keys = self.patch_log.dirty_chunks();
        let mut total = CompactionResult::noop();

        let prefix = format!("patch:{}:", table_name);

        // Get all chunks for this table once
        let all_chunks = self.catalog.all_chunks(table_name)?;

        for patch_key in dirty_keys {
            let key_str = match std::str::from_utf8(&patch_key) {
                Ok(s) => s,
                Err(_) => continue,
            };

            let row_bucket_str = match key_str.strip_prefix(&prefix) {
                Some(s) => s,
                None => continue,
            };
            let row_bucket: u64 = match row_bucket_str.parse() {
                Ok(v) => v,
                Err(_) => continue,
            };

            // Only apply patches up to the compaction snapshot
            let patches = self.patch_log.get_patches_up_to(&patch_key, compact_tx);
            if patches.is_empty() {
                continue;
            }
            let patches_count = patches.len();

            let matching_chunks: Vec<_> = all_chunks.iter()
                .filter(|(coord, _)| coord.row_bucket == row_bucket)
                .collect();

            if matching_chunks.is_empty() {
                self.patch_log.clear_patches_up_to(&patch_key, compact_tx);
                continue;
            }

            let new_version = self.catalog.next_transaction_id()?;

            for (coord, version) in &matching_chunks {
                let path = chunk_path(self.base_path, table_name, coord, *version);
                if !path.exists() {
                    continue;
                }

                let batch = read_parquet_file(&path)?;
                let bytes_before = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

                let compacted = apply_patches(batch, &patches)?;

                let new_path = chunk_path(self.base_path, table_name, coord, new_version);
                write_parquet(&new_path, &compacted, None)?;
                let bytes_after = std::fs::metadata(&new_path).map(|m| m.len()).unwrap_or(0);

                self.catalog.update_version(table_name, coord, new_version)?;

                total.chunks_compacted += 1;
                total.bytes_before += bytes_before;
                total.bytes_after += bytes_after;
            }

            total.patches_applied += patches_count;

            // Only clear patches up to compact_tx — newer patches stay for concurrent readers
            self.patch_log.clear_patches_up_to(&patch_key, compact_tx);
        }

        // Invalidate entire cache — cached entries may reference patches that were just
        // cleared from the log. A stale cache + missing patches = silent data corruption.
        // Per-key invalidation is impossible because cache keys (per-RowKey) don't match
        // patch keys (per-row_bucket). Full clear is safe and correct.
        if total.patches_applied > 0 {
            self.chunk_cache.clear();
        }

        Ok(total)
    }
}

fn read_parquet_file(path: &Path) -> Result<RecordBatch> {
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    let batches: Vec<RecordBatch> = reader
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if batches.is_empty() {
        return Err(ChunkDbError::Config("Empty parquet file during compaction".to_string()));
    }
    Ok(arrow::compute::concat_batches(&batches[0].schema(), &batches)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_result() {
        let result = CompactionResult::noop();
        assert_eq!(result.chunks_compacted, 0);
        assert_eq!(result.bytes_before, 0);
        assert_eq!(result.bytes_after, 0);
        assert_eq!(result.patches_applied, 0);
    }
}
