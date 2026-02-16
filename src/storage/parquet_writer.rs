use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::basic::Compression;
use std::fs::File;
use std::path::Path;
use crate::Result;

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


