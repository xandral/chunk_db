use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::write::batch_insert::BatchInserter;

pub struct StreamConfig {
    pub buffer_capacity: usize,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self { buffer_capacity: 10_000 }
    }
}

/// Streaming inserter over the table's hot buffer: every `write` is fsynced
/// to the WAL and immediately visible to queries; `flush` (automatic at the
/// row threshold) materializes the buffer to Parquet via BatchInserter
/// (merge-on-write).
pub struct StreamInserter {
    _schema: SchemaRef,
    /// Rows written through this handle since its last flush (the hot buffer
    /// itself is shared per table).
    buffered_rows: usize,
    config: StreamConfig,
    inserter: BatchInserter,
    total_flushed: u64,
    flush_count: u64,
}

impl StreamInserter {
    pub fn new(
        schema: SchemaRef,
        inserter: BatchInserter,
        config: StreamConfig,
    ) -> Self {
        Self {
            _schema: schema,
            buffered_rows: 0,
            config,
            inserter,
            total_flushed: 0,
            flush_count: 0,
        }
    }

    /// Buffer a batch: durable and queryable on return. Materializes to
    /// Parquet once the row threshold is crossed.
    pub fn write(&mut self, batch: &RecordBatch) -> crate::Result<Option<u64>> {
        self.inserter.buffer_insert(batch)?;
        self.buffered_rows += batch.num_rows();
        if self.buffered_rows >= self.config.buffer_capacity {
            return self.flush();
        }
        Ok(None)
    }

    /// Materialize the table's hot buffer to Parquet.
    pub fn flush(&mut self) -> crate::Result<Option<u64>> {
        let version = self.inserter.flush_hot()?;
        if version.is_some() {
            self.total_flushed += self.buffered_rows as u64;
            self.flush_count += 1;
        }
        self.buffered_rows = 0;
        Ok(version)
    }

    pub fn close(mut self) -> crate::Result<Option<u64>> {
        self.flush()
    }

    pub fn buffered_rows(&self) -> usize {
        self.buffered_rows
    }

    pub fn total_flushed_rows(&self) -> u64 {
        self.total_flushed
    }

    pub fn flush_count(&self) -> u64 {
        self.flush_count
    }
}
