use serde::{Deserialize, Serialize};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConfig {
    pub name: String,
    pub columns: Vec<ColumnConfig>,
    pub partitioning: PartitioningConfig,
    pub storage: StorageConfig,
    #[serde(default = "default_row_id_strategy")]
    pub row_id_strategy: RowIdStrategy,
}

fn default_row_id_strategy() -> RowIdStrategy {
    RowIdStrategy::Snowflake
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnConfig {
    pub name: String,
    pub data_type: String,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub primary_key: bool,
}

impl ColumnConfig {
    /// Parse data_type string to Arrow DataType
    pub fn parse_data_type(&self) -> DataType {
        parse_data_type(&self.data_type)
    }

    pub fn try_parse_data_type(&self) -> crate::Result<DataType> {
        try_parse_data_type(&self.data_type)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RowIdStrategy {
    /// Auto-generated Snowflake-like IDs
    Snowflake,
    /// Use single column as row_id (must be numeric)
    SingleColumn(String),
    /// Hash multiple columns to generate row_id
    CompositeHash(Vec<String>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitioningConfig {
    /// Rows per chunk bucket. With the adaptive grid (max_cell_rows set) this
    /// is the level-0 base cell width in row-id space — make it coarse.
    pub chunk_rows: u64,

    /// Adaptive row grid: split a row cell in half when a chunk file exceeds
    /// this many rows. None (default) = fixed grid, exact v0 behavior.
    #[serde(default)]
    pub max_cell_rows: Option<u64>,

    /// Range dimensions (timestamp, incremental IDs)
    #[serde(default)]
    pub range_dimensions: Vec<RangeDimensionConfig>,

    /// Hash dimensions (tenant_id, sensor_id)
    #[serde(default)]
    pub hash_dimensions: Vec<HashDimensionConfig>,

    /// Column groups for vertical partitioning
    #[serde(default)]
    pub column_groups: Vec<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeDimensionConfig {
    pub column: String,
    pub chunk_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashDimensionConfig {
    pub column: String,
    pub num_buckets: u64,
    #[serde(default = "default_hash_strategy")]
    pub strategy: HashStrategy,
}

/// Hash partitioning strategy
///
/// Currently only PureHash is implemented. Future strategies may include:
/// - Counter: Sequential assignment for perfect distribution (requires catalog state)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum HashStrategy {
    /// Pure hash function (xxh3) modulo num_buckets
    /// Stateless, fast, deterministic
    #[default]
    PureHash,
}

fn default_hash_strategy() -> HashStrategy {
    HashStrategy::PureHash
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub base_path: String,
}

impl TableConfig {
    /// Load from YAML file
    pub fn from_yaml(path: &str) -> crate::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        serde_yaml::from_str(&content)
            .map_err(|e| crate::ChunkDbError::Config(e.to_string()))
    }

    /// Validate every invariant relied upon by routing and physical layout.
    /// This is called both when a table is created and when persisted metadata
    /// is reopened, so malformed configuration fails before any files move.
    pub fn validate(&self) -> crate::Result<()> {
        if self.name.is_empty()
            || self.name == "."
            || self.name == ".."
            || self.name.contains('/')
            || self.name.contains('\\')
        {
            return Err(crate::ChunkDbError::Config(format!(
                "invalid table name {:?}", self.name
            )));
        }
        if self.storage.base_path.is_empty() {
            return Err(crate::ChunkDbError::Config(
                "storage.base_path must not be empty".to_string(),
            ));
        }
        if self.partitioning.chunk_rows == 0 {
            return Err(crate::ChunkDbError::Config(
                "partitioning.chunk_rows must be greater than zero".to_string(),
            ));
        }
        if self.partitioning.max_cell_rows == Some(0) {
            return Err(crate::ChunkDbError::Config(
                "partitioning.max_cell_rows must be greater than zero".to_string(),
            ));
        }
        if self.columns.is_empty() {
            return Err(crate::ChunkDbError::Config(
                "a table must define at least one user column".to_string(),
            ));
        }

        let mut names = HashSet::new();
        for column in &self.columns {
            if column.name.is_empty() || column.name == "__row_id" {
                return Err(crate::ChunkDbError::Config(format!(
                    "invalid or reserved column name {:?}", column.name
                )));
            }
            if !names.insert(column.name.as_str()) {
                return Err(crate::ChunkDbError::Config(format!(
                    "duplicate column {:?}", column.name
                )));
            }
            column.try_parse_data_type()?;
            if column.primary_key && column.nullable {
                return Err(crate::ChunkDbError::Config(format!(
                    "primary-key column {:?} cannot be nullable", column.name
                )));
            }
        }

        let find_column = |name: &str| {
            self.columns.iter().find(|column| column.name == name).ok_or_else(|| {
                crate::ChunkDbError::Config(format!(
                    "partitioning references unknown column {:?}", name
                ))
            })
        };

        let mut dimension_names = HashSet::new();
        for dimension in &self.partitioning.hash_dimensions {
            if dimension.num_buckets == 0 {
                return Err(crate::ChunkDbError::Config(format!(
                    "hash dimension {:?} must have num_buckets > 0", dimension.column
                )));
            }
            if !dimension_names.insert(dimension.column.as_str()) {
                return Err(crate::ChunkDbError::Config(format!(
                    "duplicate partition dimension {:?}", dimension.column
                )));
            }
            let column = find_column(&dimension.column)?;
            if column.nullable {
                return Err(crate::ChunkDbError::Config(format!(
                    "hash dimension {:?} cannot be nullable", dimension.column
                )));
            }
            let data_type = column.try_parse_data_type()?;
            if !matches!(
                data_type,
                DataType::Utf8
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Boolean
            ) {
                return Err(crate::ChunkDbError::Config(format!(
                    "hash dimension {:?} has unsupported type {:?}",
                    dimension.column, data_type
                )));
            }
        }

        for dimension in &self.partitioning.range_dimensions {
            if dimension.chunk_size == 0 {
                return Err(crate::ChunkDbError::Config(format!(
                    "range dimension {:?} must have chunk_size > 0", dimension.column
                )));
            }
            if !dimension_names.insert(dimension.column.as_str()) {
                return Err(crate::ChunkDbError::Config(format!(
                    "duplicate partition dimension {:?}", dimension.column
                )));
            }
            let column = find_column(&dimension.column)?;
            if column.nullable {
                return Err(crate::ChunkDbError::Config(format!(
                    "range dimension {:?} cannot be nullable", dimension.column
                )));
            }
            let data_type = column.try_parse_data_type()?;
            if !matches!(data_type, DataType::Int64 | DataType::Timestamp(_, _)) {
                return Err(crate::ChunkDbError::Config(format!(
                    "range dimension {:?} must be Int64 or Timestamp, got {:?}",
                    dimension.column, data_type
                )));
            }
        }

        if self.partitioning.column_groups.len() > u16::MAX as usize {
            return Err(crate::ChunkDbError::Config(
                "too many column groups (maximum 65535)".to_string(),
            ));
        }
        let mut grouped = HashSet::new();
        for group in &self.partitioning.column_groups {
            if group.is_empty() {
                return Err(crate::ChunkDbError::Config(
                    "column groups cannot be empty".to_string(),
                ));
            }
            for column in group {
                if !names.contains(column.as_str()) {
                    return Err(crate::ChunkDbError::Config(format!(
                        "column group references unknown column {:?}", column
                    )));
                }
                if !grouped.insert(column.as_str()) {
                    return Err(crate::ChunkDbError::Config(format!(
                        "column {:?} appears in more than one column group", column
                    )));
                }
            }
        }

        match &self.row_id_strategy {
            RowIdStrategy::Snowflake => {}
            RowIdStrategy::SingleColumn(column) => {
                find_column(column)?;
            }
            RowIdStrategy::CompositeHash(columns) => {
                if columns.is_empty() {
                    return Err(crate::ChunkDbError::Config(
                        "CompositeHash row-id strategy needs at least one column".to_string(),
                    ));
                }
                for name in columns {
                    let column = find_column(name)?;
                    let data_type = column.try_parse_data_type()?;
                    if !matches!(
                        data_type,
                        DataType::Utf8
                            | DataType::Int32
                            | DataType::Int64
                            | DataType::UInt32
                            | DataType::UInt64
                    ) {
                        return Err(crate::ChunkDbError::Config(format!(
                            "CompositeHash column {:?} has unsupported type {:?}",
                            name, data_type
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Build Arrow schema from column configs
    /// Note: Includes __row_id as the first field (matches actual Parquet structure)
    pub fn arrow_schema(&self) -> Arc<Schema> {
        let mut fields = vec![Field::new("__row_id", DataType::UInt64, false)];

        fields.extend(self.columns.iter().map(|col| {
            let dt = parse_data_type(&col.data_type);
            Field::new(&col.name, dt, col.nullable)
        }));

        Arc::new(Schema::new(fields))
    }

    /// Get column group index for a column name
    pub fn column_group_index(&self, column_name: &str) -> Option<u16> {
        for (idx, group) in self.partitioning.column_groups.iter().enumerate() {
            if group.contains(&column_name.to_string()) {
                return Some(idx as u16);
            }
        }
        // If no groups defined, all columns in group 0
        if self.partitioning.column_groups.is_empty() {
            return Some(0);
        }
        None
    }

    /// Get primary key columns (marked with primary_key = true)
    pub fn primary_key_columns(&self) -> Vec<String> {
        self.columns.iter()
            .filter(|c| c.primary_key)
            .map(|c| c.name.clone())
            .collect()
    }

    /// Auto-detect row_id strategy from primary keys if not explicitly set
    pub fn infer_row_id_strategy(&mut self) {
        let pk_cols = self.primary_key_columns();

        if pk_cols.is_empty() {
            // No PK defined, use Snowflake
            self.row_id_strategy = RowIdStrategy::Snowflake;
        } else if pk_cols.len() == 1 {
            // Single PK column
            let col_name = &pk_cols[0];
            let col_config = self.columns.iter().find(|c| &c.name == col_name).unwrap();

            // Check if it's numeric
            let is_numeric = matches!(
                col_config.data_type.to_lowercase().as_str(),
                "int64" | "uint64" | "int32" | "uint32" | "int16" | "uint16" | "int8" | "uint8"
            );

            if is_numeric {
                self.row_id_strategy = RowIdStrategy::SingleColumn(col_name.clone());
            } else {
                // String or other type, use hash
                self.row_id_strategy = RowIdStrategy::CompositeHash(vec![col_name.clone()]);
            }
        } else {
            // Composite PK, use hash
            self.row_id_strategy = RowIdStrategy::CompositeHash(pk_cols);
        }
    }
}

fn parse_data_type(s: &str) -> DataType {
    // Kept as an infallible compatibility helper. Database entry points call
    // `validate` first, so the fallback is unreachable for stored tables.
    try_parse_data_type(s).unwrap_or(DataType::Utf8)
}

fn try_parse_data_type(s: &str) -> crate::Result<DataType> {
    match s.to_lowercase().as_str() {
        "int8" => Ok(DataType::Int8),
        "int16" => Ok(DataType::Int16),
        "int32" => Ok(DataType::Int32),
        "int64" => Ok(DataType::Int64),
        "uint8" => Ok(DataType::UInt8),
        "uint16" => Ok(DataType::UInt16),
        "uint32" => Ok(DataType::UInt32),
        "uint64" => Ok(DataType::UInt64),
        "float32" | "float" => Ok(DataType::Float32),
        "float64" | "double" => Ok(DataType::Float64),
        "utf8" | "string" => Ok(DataType::Utf8),
        "bool" | "boolean" => Ok(DataType::Boolean),
        "date32" | "date" => Ok(DataType::Date32),
        "timestamp" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        _ => Err(crate::ChunkDbError::Config(format!(
            "unknown column data type {:?}", s
        ))),
    }
}

