use arrow::array::{RecordBatch, UInt64Array};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::catalog::{VersionCatalog, HashRegistry};
use crate::config::table_config::TableConfig;
use crate::partitioning::{ColumnGroupMapper, row_bucket};
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
}

/// Main database handle
pub struct ChunkDb {
    base_path: PathBuf,
    catalog_db: Arc<sled::Db>,
    version_catalog: Arc<VersionCatalog>,
    tables: HashMap<String, TableState>,
    patch_log: Arc<PatchLog>,
    chunk_cache: Arc<ChunkCache>,
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
                // Recreate hash registries and column mapper
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

                let table_state = TableState {
                    config,
                    hash_registries,
                    column_mapper,
                };

                tables.insert(table_name, table_state);
            }
        }

        let patch_log = Arc::new(PatchLog::new());
        let chunk_cache = Arc::new(ChunkCache::new(1024));

        Ok(Self {
            base_path,
            catalog_db,
            version_catalog,
            tables,
            patch_log,
            chunk_cache,
        })
    }

    /// Create a table from configuration
    pub fn create_table(&mut self, config: TableConfig) -> Result<()> {
        let table_name = config.name.clone();

        // Create table directory
        let table_path = self.base_path.join(&table_name);
        std::fs::create_dir_all(table_path.join("chunks"))?;

        // Create hash registries and column mapper
        let hash_registries: Vec<HashRegistry> = config
            .partitioning
            .hash_dimensions
            .iter()
            .map(|dim| {
                HashRegistry::new(
                    self.catalog_db.clone(),
                    &dim.column,
                    dim.num_buckets,
                    dim.strategy.clone(),
                )
            })
            .collect();

        let column_mapper = ColumnGroupMapper::new(&config);

        let table_state = TableState {
            config: config.clone(),
            hash_registries,
            column_mapper,
        };

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

        Ok(BatchInserter::new(
            table_state.config.clone(),
            self.version_catalog.clone(),
            self.catalog_db.clone(),
        ))
    }

    /// Insert a batch into a table
    pub fn insert(&self, table_name: &str, batch: &RecordBatch) -> Result<u64> {
        let inserter = self.inserter(table_name)?;
        inserter.insert(batch)
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
    pub fn select(&self, columns: &[&str]) -> QueryBuilder {
        QueryBuilder::new(self).select(columns)
    }

    /// Start a query selecting all columns from a table
    pub fn select_all(&self, table_name: &str) -> QueryBuilder {
        QueryBuilder::new(self).select_all().from(table_name)
    }

    /// Delete rows by their __row_id values
    pub fn delete_rows(&self, table_name: &str, row_ids: &[u64]) -> Result<u64> {
        let table_state = self.tables.get(table_name)
            .ok_or_else(|| ChunkDbError::Config(format!("Table '{}' not found", table_name)))?;

        let tx_id = self.version_catalog.next_transaction_id()?;
        let chunk_rows = table_state.config.partitioning.chunk_rows;

        // Group row_ids by row_bucket
        let mut by_bucket: HashMap<u64, Vec<u64>> = HashMap::new();
        for &rid in row_ids {
            let bucket = row_bucket(rid, chunk_rows);
            by_bucket.entry(bucket).or_default().push(rid);
        }

        for (bucket, ids) in by_bucket {
            let key = patch_key(table_name, bucket);
            self.patch_log.record(&key, tx_id, PatchOp::Delete(ids))?;
        }

        Ok(tx_id)
    }

    /// Update rows. The batch must contain __row_id (UInt64) and all columns of the table.
    pub fn update_rows(&self, table_name: &str, batch: &RecordBatch) -> Result<u64> {
        let table_state = self.tables.get(table_name)
            .ok_or_else(|| ChunkDbError::Config(format!("Table '{}' not found", table_name)))?;

        let tx_id = self.version_catalog.next_transaction_id()?;
        let chunk_rows = table_state.config.partitioning.chunk_rows;

        let row_id_idx = batch.schema().index_of("__row_id")?;
        let row_ids = batch.column(row_id_idx).as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| ChunkDbError::Config("__row_id must be UInt64".into()))?;

        // Group row indices by row_bucket
        let mut by_bucket: HashMap<u64, Vec<usize>> = HashMap::new();
        for i in 0..row_ids.len() {
            let bucket = row_bucket(row_ids.value(i), chunk_rows);
            by_bucket.entry(bucket).or_default().push(i);
        }

        for (bucket, indices) in by_bucket {
            let sub_batch = take_rows(batch, &indices)?;
            let key = patch_key(table_name, bucket);
            self.patch_log.record(&key, tx_id, PatchOp::Update(sub_batch))?;
        }

        Ok(tx_id)
    }

    /// Run compaction on all dirty chunks for a table
    pub fn compact(&self, table_name: &str) -> Result<crate::write::CompactionResult> {
        let compactor = crate::write::Compactor::new(
            &self.version_catalog,
            &self.patch_log,
            &self.chunk_cache,
            &self.base_path,
        );
        compactor.compact_all(table_name)
    }

    /// Get a reference to the patch log (for advanced usage)
    pub fn patch_log(&self) -> &Arc<PatchLog> {
        &self.patch_log
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

        let inserter = BatchInserter::new(
            table_state.config.clone(),
            self.version_catalog.clone(),
            self.catalog_db.clone(),
        );

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
        let table = table_name.to_string();

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
                    let compactor = Compactor::new(
                        &version_catalog,
                        &patch_log,
                        &chunk_cache,
                        &base_path,
                    );
                    let _ = compactor.compact_all(&table);
                }
            }
        });

        CompactionHandle::new(shutdown, trigger, join_handle)
    }
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

/// Build a patch key from table name and row_bucket
pub(crate) fn patch_key(table_name: &str, row_bucket: u64) -> Vec<u8> {
    format!("patch:{}:{}", table_name, row_bucket).into_bytes()
}

/// Extract specific rows from a RecordBatch by index
fn take_rows(batch: &RecordBatch, indices: &[usize]) -> Result<RecordBatch> {
    let indices_arr = arrow::array::UInt32Array::from_iter_values(
        indices.iter().map(|&i| i as u32)
    );
    let columns: Result<Vec<arrow::array::ArrayRef>> = batch.columns().iter()
        .map(|col| Ok(arrow::compute::take(col.as_ref(), &indices_arr, None)?))
        .collect();
    Ok(RecordBatch::try_new(batch.schema(), columns?)?)
}
