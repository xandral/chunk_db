use arrow::array::RecordBatch;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::catalog::{VersionCatalog, HashRegistry};
use crate::config::table_config::TableConfig;
use crate::partitioning::ColumnGroupMapper;
use crate::query::{DirectExecutor, QueryBuilder};
use crate::write::BatchInserter;
use crate::Result;

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

        Ok(Self {
            base_path,
            catalog_db,
            version_catalog,
            tables,
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
    pub(crate) fn create_executor(&self, table_name: &str) -> Result<DirectExecutor> {
        let table_state = self.tables.get(table_name)
            .ok_or_else(|| crate::ChunkDbError::Config(
                format!("Table '{}' not found", table_name)
            ))?;

        Ok(DirectExecutor::new(
            Arc::new(table_state.config.clone()),
            self.version_catalog.clone(),
            Arc::new(table_state.hash_registries.clone()),
            Arc::new(table_state.column_mapper.clone()),
            self.base_path.clone(),
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


