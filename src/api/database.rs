use arrow::array::RecordBatch;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::catalog::{VersionCatalog, HashRegistry};
use crate::concurrency::CompactionLock;
use crate::config::table_config::TableConfig;
use crate::partitioning::{ColumnGroupMapper, DimensionMap, LevelMap, SplitAxis};
use crate::query::{DirectExecutor, QueryBuilder};
use crate::storage::ChunkCache;
use crate::write::{BatchInserter, PatchLog, PatchOp, StreamInserter, StreamConfig};
use crate::write::{AutoCompactionConfig, CompactionHandle, Compactor};
use crate::{ChunkDbError, Result};

/// Per-table state (read-only after creation)
struct TableState {
    config: TableConfig,
    hash_registries: Vec<HashRegistry>,
    column_mapper: ColumnGroupMapper,
    /// Adaptive row grid refinement map (empty = pure level-0 fixed grid)
    level_map: Arc<LevelMap>,
    /// Local hash/range refinement tree layered inside each row leaf.
    dimension_map: Arc<DimensionMap>,
    /// Serializes merge-on-write operations for this table. Without this,
    /// concurrent writers can both read the same previous chunk version and
    /// whichever catalog update lands last silently loses the other batch.
    write_lock: Arc<Mutex<()>>,
}

/// Main database handle
pub struct ChunkDb {
    base_path: PathBuf,
    catalog_db: Arc<sled::Db>,
    version_catalog: Arc<VersionCatalog>,
    tables: HashMap<String, TableState>,
    patch_log: Arc<PatchLog>,
    chunk_cache: Arc<ChunkCache>,
    /// Per-cell mutual exclusion between compaction and cell splits
    compaction_lock: Arc<CompactionLock>,
}

/// Build the per-table runtime state (hash registries, column mapper, level
/// map) from a persisted config — shared by `open` and `create_table`.
fn build_table_state(
    catalog_db: &Arc<sled::Db>,
    version_catalog: &VersionCatalog,
    config: TableConfig,
) -> Result<TableState> {
    config.validate()?;

    let hash_registries: Vec<HashRegistry> = config
        .partitioning
        .hash_dimensions
        .iter()
        .map(|dim| {
            HashRegistry::new(
                catalog_db.clone(),
                &dim.column,
                dim.num_buckets,
                dim.strategy.clone(),
            )
        })
        .collect();

    let column_mapper = ColumnGroupMapper::new(&config);

    let level_map = Arc::new(LevelMap::from_snapshot(
        config.partitioning.chunk_rows,
        version_catalog.load_level_map(&config.name)?,
    )?);

    let hash_base_buckets: Vec<u64> = config.partitioning.hash_dimensions
        .iter()
        .map(|dimension| dimension.num_buckets)
        .collect();
    let range_chunk_sizes: Vec<u64> = config.partitioning.range_dimensions
        .iter()
        .map(|dimension| dimension.chunk_size)
        .collect();
    let dimension_map = Arc::new(DimensionMap::from_snapshot(
        &hash_base_buckets,
        &range_chunk_sizes,
        version_catalog.load_dimension_map(&config.name)?,
    )?);

    Ok(TableState {
        config,
        hash_registries,
        column_mapper,
        level_map,
        dimension_map,
        write_lock: Arc::new(Mutex::new(())),
    })
}

impl ChunkDb {
    /// Open or create a ChunkDB instance
    pub fn open(base_path: &str) -> Result<Self> {
        let base_path = PathBuf::from(base_path);
        std::fs::create_dir_all(&base_path)?;

        let catalog_path = base_path.join("catalog");
        let catalog_db = Arc::new(sled::open(&catalog_path)?);
        let version_catalog = Arc::new(VersionCatalog::open(catalog_path.join("versions"))?);

        // Load existing tables from catalog
        let mut tables = HashMap::new();
        let table_names = version_catalog.list_tables()?;

        for table_name in table_names {
            if let Some(config) = version_catalog.load_table_config(&table_name)? {
                let table_state = build_table_state(&catalog_db, &version_catalog, config)?;
                tables.insert(table_name, table_state);
            }
        }

        // Durable patch log: updates/deletes are WAL-backed and survive a
        // crash (replayed here); compaction/splits materialize and clear them.
        let patch_log = Arc::new(PatchLog::with_wal(
            &base_path.join("wal").join("patches.wal"),
        )?);
        let chunk_cache = Arc::new(ChunkCache::new(1024));

        Ok(Self {
            base_path,
            catalog_db,
            version_catalog,
            tables,
            patch_log,
            chunk_cache,
            compaction_lock: Arc::new(CompactionLock::new()),
        })
    }

    /// Create a table from configuration
    pub fn create_table(&mut self, config: TableConfig) -> Result<()> {
        let table_name = config.name.clone();

        // Create table directory
        let table_path = self.base_path.join(&table_name);
        std::fs::create_dir_all(table_path.join("chunks"))?;

        let table_state = build_table_state(&self.catalog_db, &self.version_catalog, config.clone())?;
        self.tables.insert(table_name, table_state);

        // Persist table configuration to catalog
        self.version_catalog.save_table_config(&config)?;

        Ok(())
    }

    /// Create a table from YAML config file
    pub fn create_table_from_yaml(&mut self, path: &str) -> Result<()> {
        let config = TableConfig::from_yaml(path)?;
        self.create_table(config)
    }

    /// Get inserter for a table
    pub fn inserter(&self, table_name: &str) -> Result<BatchInserter> {
        let table_state = self.tables.get(table_name)
            .ok_or_else(|| crate::ChunkDbError::Config(
                format!("Table '{}' not found", table_name)
            ))?;

        Ok(self.build_inserter(table_state))
    }

    fn build_inserter(&self, table_state: &TableState) -> BatchInserter {
        BatchInserter::new_with_write_lock(
            table_state.config.clone(),
            self.version_catalog.clone(),
            self.catalog_db.clone(),
            table_state.level_map.clone(),
            table_state.dimension_map.clone(),
            self.patch_log.clone(),
            self.chunk_cache.clone(),
            self.compaction_lock.clone(),
            table_state.write_lock.clone(),
        )
    }

    /// Insert a batch into a table
    pub fn insert(&self, table_name: &str, batch: &RecordBatch) -> Result<u64> {
        let inserter = self.inserter(table_name)?;
        inserter.insert(batch)
    }

    /// Buffered insert (hot buffer): the batch is fsynced to the WAL and
    /// immediately visible to queries, but not yet materialized to Parquet.
    /// Much cheaper than `insert` (no merge-on-write); materialize with
    /// `flush_hot_buffer` / `compact`, or via a `stream_inserter` threshold.
    pub fn insert_buffered(&self, table_name: &str, batch: &RecordBatch) -> Result<u64> {
        let inserter = self.inserter(table_name)?;
        inserter.buffer_insert(batch)
    }

    /// Materialize the table's hot buffer into chunk files through the normal
    /// insert path (merge-on-write, adaptive-grid splits included). Returns
    /// the version written, or None if the buffer was empty.
    pub fn flush_hot_buffer(&self, table_name: &str) -> Result<Option<u64>> {
        let inserter = self.inserter(table_name)?;
        inserter.flush_hot()
    }

    /// Create a direct executor for a table (internal use)
    ///
    /// PERF: Clones config, hash_registries, and column_mapper on every query.
    /// These are read-only after table creation — could be stored as Arc in TableState
    /// to avoid the clone overhead.
    pub fn create_executor(&self, table_name: &str) -> Result<DirectExecutor> {
        let table_state = self.tables.get(table_name)
            .ok_or_else(|| crate::ChunkDbError::Config(
                format!("Table '{}' not found", table_name)
            ))?;

        let snapshot_tx_id = self.version_catalog.current_transaction_id();

        Ok(DirectExecutor::new(
            Arc::new(table_state.config.clone()),
            self.version_catalog.clone(),
            Arc::new(table_state.hash_registries.clone()),
            Arc::new(table_state.column_mapper.clone()),
            self.base_path.clone(),
            self.patch_log.clone(),
            self.chunk_cache.clone(),
            snapshot_tx_id,
        ))
    }

    /// Start a query with column selection
    pub fn select(&self, columns: &[&str]) -> QueryBuilder<'_> {
        QueryBuilder::new(self).select(columns)
    }

    /// Start a query selecting all columns from a table
    pub fn select_all(&self, table_name: &str) -> QueryBuilder<'_> {
        QueryBuilder::new(self).select_all().from(table_name)
    }

    /// Delete rows by their __row_id values
    pub fn delete_rows(&self, table_name: &str, row_ids: &[u64]) -> Result<u64> {
        let table_state = self.tables.get(table_name)
            .ok_or_else(|| ChunkDbError::Config(format!("Table '{}' not found", table_name)))?;

        // Cell patches only reach materialized rows — flush the hot buffer
        // first so buffered rows can be deleted too.
        self.flush_hot_buffer(table_name)?;

        let tx_id = self.version_catalog.next_transaction_id()?;

        // Group row_ids by leaf row cell. The routing guard stays alive until
        // the patches are recorded: a concurrent split cannot mark this cell
        // refined (and drain its patches) in between.
        let router = table_state.level_map.routing();
        let mut by_cell: HashMap<(u16, u64), Vec<u64>> = HashMap::new();
        for &rid in row_ids {
            let cell = router.route(rid);
            by_cell.entry(cell).or_default().push(rid);
        }

        for ((level, bucket), ids) in by_cell {
            let key = patch_key(table_name, level, bucket);
            self.patch_log.record(&key, tx_id, PatchOp::Delete(ids))?;
        }
        drop(router);

        Ok(tx_id)
    }

    /// Update rows. The batch must contain __row_id (UInt64) and all columns of the table.
    pub fn update_rows(&self, table_name: &str, batch: &RecordBatch) -> Result<u64> {
        // Cell patches only reach materialized rows — flush the hot buffer
        // first so buffered rows can be updated too.
        self.flush_hot_buffer(table_name)?;
        self.inserter(table_name)?.update_rows(batch)
    }

    /// Run compaction on all dirty chunks for a table. Also materializes the
    /// hot buffer first, so "after compact everything lives in Parquet and
    /// the WAL is empty" stays true.
    pub fn compact(&self, table_name: &str) -> Result<crate::write::CompactionResult> {
        let table_state = self.tables.get(table_name)
            .ok_or_else(|| ChunkDbError::Config(format!("Table '{}' not found", table_name)))?;
        self.flush_hot_buffer(table_name)?;
        let compactor = crate::write::Compactor::new(
            &self.version_catalog,
            &self.patch_log,
            &self.chunk_cache,
            &self.base_path,
            &self.compaction_lock,
            &table_state.config,
        );
        let mut result = {
            // Compaction is a read/modify/write of the same base files as an
            // insert. Sharing the table writer lock prevents either catalog
            // update from silently winning over the other.
            let _write_guard = table_state.write_lock.lock().unwrap();
            compactor.compact_all(table_name)?
        };
        let rebalance = self.build_inserter(table_state).rebalance_underfilled()?;
        result.cells_merged = rebalance.cells_merged;
        Ok(result)
    }

    /// Coalesce underfilled local hash/range siblings without running patch
    /// compaction first. Cells with pending patches are skipped safely.
    pub fn rebalance(&self, table_name: &str) -> Result<crate::write::RebalanceResult> {
        let table_state = self.tables.get(table_name)
            .ok_or_else(|| ChunkDbError::Config(format!("Table '{}' not found", table_name)))?;
        self.build_inserter(table_state).rebalance_underfilled()
    }

    /// Get a reference to the patch log (for advanced usage)
    pub fn patch_log(&self) -> &Arc<PatchLog> {
        &self.patch_log
    }

    /// Filenames of the chunks currently referenced by the catalog (latest
    /// version per live coordinate). Files on disk not in this list are
    /// orphans: superseded versions or pre-split parents.
    pub fn live_chunk_files(&self, table_name: &str) -> Result<Vec<String>> {
        let chunks = self.version_catalog.all_chunks(table_name)?;
        Ok(chunks.iter()
            .map(|(coord, version)| crate::storage::format_chunk_filename(coord, *version))
            .collect())
    }

    /// Inspect the currently persisted adaptive-grid shape without scanning
    /// Parquet data.
    pub fn adaptive_grid_stats(&self, table_name: &str) -> Result<AdaptiveGridStats> {
        let table_state = self.tables.get(table_name)
            .ok_or_else(|| ChunkDbError::Config(format!("Table '{}' not found", table_name)))?;
        let chunks = self.version_catalog.all_chunks(table_name)?;
        let logical_cells: std::collections::HashSet<_> = chunks.iter()
            .map(|(coord, _)| coord.cell())
            .collect();
        let dimension_snapshot = table_state.dimension_map.snapshot();

        let mut stats = AdaptiveGridStats {
            row_internal_cells: table_state.level_map.snapshot().len(),
            local_internal_cells: dimension_snapshot.len(),
            logical_leaf_cells: logical_cells.len(),
            physical_chunks: chunks.len(),
            ..AdaptiveGridStats::default()
        };
        for (cell, axis) in dimension_snapshot {
            match axis {
                SplitAxis::Hash(_) => stats.hash_splits += 1,
                SplitAxis::Range(_) => stats.range_splits += 1,
            }
            stats.max_hash_level = stats.max_hash_level
                .max(cell.hash_levels.iter().copied().max().unwrap_or(0));
            stats.max_range_level = stats.max_range_level
                .max(cell.range_levels.iter().copied().max().unwrap_or(0));
        }
        stats.max_row_level = chunks.iter()
            .map(|(coord, _)| coord.level)
            .max()
            .unwrap_or(0);
        for cell in logical_cells {
            stats.max_hash_level = stats.max_hash_level
                .max(cell.hash_levels.iter().copied().max().unwrap_or(0));
            stats.max_range_level = stats.max_range_level
                .max(cell.range_levels.iter().copied().max().unwrap_or(0));
        }
        Ok(stats)
    }

    /// Delete orphaned chunk files: superseded versions and pre-split parents
    /// no longer referenced by the catalog. Files younger than `min_age` are
    /// kept — a concurrent insert writes its Parquet before registering it in
    /// the catalog, so a fresh unreferenced file may simply not be committed
    /// yet. Maintenance operation: a long-running query that snapshotted its
    /// chunk list before the GC can fail if its files are collected under it.
    pub fn collect_garbage(&self, table_name: &str, min_age: std::time::Duration) -> Result<GcResult> {
        use std::collections::HashSet;

        let chunks_dir = self.base_path.join(table_name).join("chunks");
        if !chunks_dir.exists() {
            return Ok(GcResult::default());
        }

        // Directory listing first, live set second: anything committed after
        // the listing isn't in it, so it can't be deleted by mistake.
        let entries: Vec<_> = std::fs::read_dir(&chunks_dir)?
            .collect::<std::io::Result<Vec<_>>>()?;
        let live: HashSet<String> = self.live_chunk_files(table_name)?.into_iter().collect();

        let now = std::time::SystemTime::now();
        let mut result = GcResult::default();

        for entry in entries {
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.ends_with(".parquet") || live.contains(&name) {
                continue;
            }

            let meta = entry.metadata()?;
            let age = meta.modified().ok()
                .and_then(|m| now.duration_since(m).ok())
                .unwrap_or_default();
            if age < min_age {
                result.files_kept_young += 1;
                continue;
            }

            std::fs::remove_file(entry.path())?;
            result.files_removed += 1;
            result.bytes_reclaimed += meta.len();
        }

        Ok(result)
    }

    /// Create a streaming inserter that buffers small batches and flushes
    /// them to disk via BatchInserter (merge-on-write).
    pub fn stream_inserter(&self, table_name: &str, config: StreamConfig) -> Result<StreamInserter> {
        let table_state = self.tables.get(table_name)
            .ok_or_else(|| ChunkDbError::Config(format!("Table '{}' not found", table_name)))?;

        // User-facing schema (without __row_id — BatchInserter generates it)
        let full_schema = table_state.config.arrow_schema();
        let user_fields: Vec<_> = full_schema.fields().iter()
            .filter(|f| f.name() != "__row_id")
            .cloned()
            .collect();
        let user_schema = Arc::new(arrow::datatypes::Schema::new(user_fields));

        let inserter = self.build_inserter(table_state);

        Ok(StreamInserter::new(user_schema, inserter, config))
    }

    /// Start a background auto-compaction task for a table.
    /// Returns a handle to control the compaction loop (trigger, shutdown).
    pub fn start_auto_compaction(&self, table_name: &str, config: AutoCompactionConfig) -> CompactionHandle {
        let shutdown = Arc::new(AtomicBool::new(false));
        let trigger = Arc::new(tokio::sync::Notify::new());

        let s = shutdown.clone();
        let t = trigger.clone();
        let patch_log = self.patch_log.clone();
        let chunk_cache = self.chunk_cache.clone();
        let version_catalog = self.version_catalog.clone();
        let base_path = self.base_path.clone();
        let compaction_lock = self.compaction_lock.clone();
        let table = table_name.to_string();
        let table_runtime = self.tables.get(table_name)
            .map(|state| (state.config.clone(), state.write_lock.clone()));

        let join_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(config.check_interval) => {},
                    _ = t.notified() => {},
                }

                if s.load(Ordering::Relaxed) {
                    break;
                }

                // Check if compaction is needed
                let total = patch_log.total_entries();
                if total >= config.max_total_patches {
                    let Some((table_config, write_lock)) = table_runtime.as_ref() else {
                        break;
                    };
                    let compactor = Compactor::new(
                        &version_catalog,
                        &patch_log,
                        &chunk_cache,
                        &base_path,
                        &compaction_lock,
                        table_config,
                    );
                    let _ = {
                        let _write_guard = write_lock.lock().unwrap();
                        compactor.compact_all(&table)
                    };
                }
            }
        });

        CompactionHandle::new(shutdown, trigger, join_handle)
    }
}

/// Outcome of a `collect_garbage` run
#[derive(Debug, Default)]
pub struct GcResult {
    pub files_removed: usize,
    pub bytes_reclaimed: u64,
    /// Unreferenced files skipped because they were younger than `min_age`
    pub files_kept_young: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AdaptiveGridStats {
    pub row_internal_cells: usize,
    pub local_internal_cells: usize,
    pub hash_splits: usize,
    pub range_splits: usize,
    pub logical_leaf_cells: usize,
    pub physical_chunks: usize,
    pub max_row_level: u16,
    pub max_hash_level: u16,
    pub max_range_level: u16,
}

/// Builder for creating tables programmatically
pub struct TableBuilder {
    name: String,
    columns: Vec<crate::config::table_config::ColumnConfig>,
    partitioning: crate::config::table_config::PartitioningConfig,
    storage_path: String,
    row_id_strategy: Option<crate::config::table_config::RowIdStrategy>,
}

impl TableBuilder {
    pub fn new(name: &str, storage_path: &str) -> Self {
        Self {
            name: name.to_string(),
            columns: vec![],
            partitioning: crate::config::table_config::PartitioningConfig {
                chunk_rows: 100_000,
                max_cell_rows: None,
                range_dimensions: vec![],
                hash_dimensions: vec![],
                column_groups: vec![],
            },
            storage_path: storage_path.to_string(),
            row_id_strategy: None,  // Will be inferred from primary keys
        }
    }

    pub fn add_column(mut self, name: &str, data_type: &str, nullable: bool) -> Self {
        self.columns.push(crate::config::table_config::ColumnConfig {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable,
            primary_key: false,
        });
        self
    }

    pub fn add_primary_key(mut self, name: &str, data_type: &str) -> Self {
        self.columns.push(crate::config::table_config::ColumnConfig {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable: false,
            primary_key: true,
        });
        self
    }

    pub fn chunk_rows(mut self, rows: u64) -> Self {
        self.partitioning.chunk_rows = rows;
        self
    }

    /// Enable the adaptive row grid: split a row cell when a chunk file
    /// exceeds `rows`. Pair with a coarse `chunk_rows` base width.
    pub fn max_cell_rows(mut self, rows: u64) -> Self {
        self.partitioning.max_cell_rows = Some(rows);
        self
    }

    pub fn add_range_dimension(mut self, column: &str, chunk_size: u64) -> Self {
        self.partitioning.range_dimensions.push(
            crate::config::table_config::RangeDimensionConfig {
                column: column.to_string(),
                chunk_size,
            }
        );
        self
    }

    pub fn add_hash_dimension(mut self, column: &str, num_buckets: u64) -> Self {
        self.partitioning.hash_dimensions.push(
            crate::config::table_config::HashDimensionConfig {
                column: column.to_string(),
                num_buckets,
                strategy: crate::config::table_config::HashStrategy::PureHash,
            }
        );
        self
    }

    pub fn add_column_group(mut self, columns: Vec<&str>) -> Self {
        self.partitioning.column_groups.push(
            columns.into_iter().map(String::from).collect()
        );
        self
    }

    pub fn with_snowflake_id(mut self) -> Self {
        self.row_id_strategy = Some(crate::config::table_config::RowIdStrategy::Snowflake);
        self
    }

    pub fn with_primary_key_as_row_id(mut self, column: &str) -> Self {
        self.row_id_strategy = Some(crate::config::table_config::RowIdStrategy::SingleColumn(column.to_string()));
        self
    }

    pub fn with_composite_key_as_row_id(mut self, columns: Vec<&str>) -> Self {
        self.row_id_strategy = Some(crate::config::table_config::RowIdStrategy::CompositeHash(
            columns.into_iter().map(String::from).collect()
        ));
        self
    }

    pub fn build(self) -> TableConfig {
        let mut config = TableConfig {
            name: self.name,
            columns: self.columns,
            partitioning: self.partitioning,
            storage: crate::config::table_config::StorageConfig {
                base_path: self.storage_path,
            },
            row_id_strategy: crate::config::table_config::RowIdStrategy::Snowflake,  // Default
        };

        // Apply explicit strategy or infer from primary keys
        if let Some(strategy) = self.row_id_strategy {
            config.row_id_strategy = strategy;
        } else {
            config.infer_row_id_strategy();
        }

        config
    }
}

/// Build a patch key from table name and row cell (level + bucket)
pub(crate) fn patch_key(table_name: &str, level: u16, row_bucket: u64) -> Vec<u8> {
    format!("patch:{}:{}:{}", table_name, level, row_bucket).into_bytes()
}

/// Patch-log key holding a table's hot buffer (WAL-backed buffered inserts).
/// Deliberately outside the `patch:` namespace: the compactor's per-cell walk
/// skips it, and no cell scan ever applies it as a chunk patch.
pub(crate) fn hot_buffer_key(table_name: &str) -> Vec<u8> {
    format!("hot:{}", table_name).into_bytes()
}
