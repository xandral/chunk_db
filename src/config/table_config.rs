use serde::{Deserialize, Serialize};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

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
    /// Rows per chunk bucket
    pub chunk_rows: u64,

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
    match s.to_lowercase().as_str() {
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float32" | "float" => DataType::Float32,
        "float64" | "double" => DataType::Float64,
        "utf8" | "string" => DataType::Utf8,
        "bool" | "boolean" => DataType::Boolean,
        "date32" | "date" => DataType::Date32,
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        // BUG: Silent fallback to Utf8 for unknown types. Could cause subtle data corruption
        // if a user misspells a type name. Should return an error instead.
        _ => DataType::Utf8,
    }
}


