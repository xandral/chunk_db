use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::compute::{lexsort_to_indices, take, SortColumn};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::format::SortingColumn as ParquetSortingColumn;
use parquet::schema::types::ColumnPath;
use parquet::basic::Compression;
use std::collections::HashSet;
use std::fs::File;
use std::path::Path;
use crate::Result;
use crate::config::TableConfig;

const DEFAULT_MAX_ROW_GROUP_ROWS: usize = 8_192;

/// Sort a physical column-group batch and write it with bounded row groups and
/// Bloom filters for equality-pruned keys.
///
/// Range dimensions lead the sort order, followed by hash dimensions and
/// `__row_id`. Only columns present in this physical group participate.
pub fn write_table_parquet<P: AsRef<Path>>(
    path: P,
    batch: &RecordBatch,
    config: &TableConfig,
) -> Result<()> {
    let sort_indices = physical_sort_indices(batch, config);
    let sorted = sort_batch(batch, &sort_indices)?;

    let configured_limit = config.partitioning.max_cell_rows
        .unwrap_or(config.partitioning.chunk_rows)
        .min(DEFAULT_MAX_ROW_GROUP_ROWS as u64)
        .max(1) as usize;
    let mut properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_write_batch_size(configured_limit.min(DEFAULT_MAX_ROW_GROUP_ROWS))
        .set_max_row_group_size(configured_limit);

    let schema = batch.schema();
    if !sort_indices.is_empty() {
        let sorting_columns = sort_indices.iter()
            .map(|&index| ParquetSortingColumn::new(index as i32, false, false))
            .collect();
        properties = properties.set_sorting_columns(Some(sorting_columns));
    }

    let mut bloom_columns = HashSet::new();
    bloom_columns.insert("__row_id");
    for dimension in &config.partitioning.hash_dimensions {
        bloom_columns.insert(dimension.column.as_str());
    }
    for name in bloom_columns {
        if schema.index_of(name).is_ok() {
            properties = properties.set_column_bloom_filter_enabled(
                ColumnPath::from(name),
                true,
            );
        }
    }

    write_parquet(path, &sorted, Some(properties.build()))
}

fn physical_sort_indices(batch: &RecordBatch, config: &TableConfig) -> Vec<usize> {
    let schema = batch.schema();
    let mut names = Vec::new();
    names.extend(config.partitioning.range_dimensions.iter()
        .map(|dimension| dimension.column.as_str()));
    names.extend(config.partitioning.hash_dimensions.iter()
        .map(|dimension| dimension.column.as_str()));
    names.push("__row_id");

    let mut seen = HashSet::new();
    names.into_iter()
        .filter(|name| seen.insert(*name))
        .filter_map(|name| schema.index_of(name).ok())
        .collect()
}

fn sort_batch(batch: &RecordBatch, sort_indices: &[usize]) -> Result<RecordBatch> {
    if batch.num_rows() <= 1 || sort_indices.is_empty() {
        return Ok(batch.clone());
    }
    let sort_columns: Vec<_> = sort_indices.iter()
        .map(|&index| SortColumn {
            values: batch.column(index).clone(),
            options: None,
        })
        .collect();
    let indices = lexsort_to_indices(&sort_columns, None)?;
    let columns = batch.columns().iter()
        .map(|column| take(column.as_ref(), &indices, None).map_err(Into::into))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

/// Write a RecordBatch to a Parquet file
pub fn write_parquet<P: AsRef<Path>>(
    path: P,
    batch: &RecordBatch,
    properties: Option<WriterProperties>,
) -> Result<()> {
    // Ensure parent directory exists
    if let Some(parent) = path.as_ref().parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = File::create(path)?;
    let props = properties.unwrap_or_else(|| {
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_write_batch_size(8192)
            .build()
    });

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;

    Ok(())
}

/// Write multiple batches to a single Parquet file
pub fn write_parquet_batches<P: AsRef<Path>>(
    path: P,
    schema: SchemaRef,
    batches: &[RecordBatch],
    properties: Option<WriterProperties>,
) -> Result<()> {
    if let Some(parent) = path.as_ref().parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = File::create(path)?;
    let props = properties.unwrap_or_else(|| {
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build()
    });

    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;

    for batch in batches {
        writer.write(batch)?;
    }

    writer.close()?;

    Ok(())
}

