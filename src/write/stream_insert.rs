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

/// Streaming inserter that buffers small batches and flushes them to disk
/// via BatchInserter (merge-on-write). Data is immediately materialized to parquet.
pub struct StreamInserter {
    _schema: SchemaRef,
    buffer: Vec<RecordBatch>,
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
            buffer: Vec::new(),
            buffered_rows: 0,
            config,
            inserter,
            total_flushed: 0,
            flush_count: 0,
        }
    }

    pub fn write(&mut self, batch: &RecordBatch) -> crate::Result<Option<u64>> {
        self.buffer.push(batch.clone());
        self.buffered_rows += batch.num_rows();
        if self.buffered_rows >= self.config.buffer_capacity {
            return self.flush();
        }
        Ok(None)
    }

    /// Flush buffered data to disk via BatchInserter.
    pub fn flush(&mut self) -> crate::Result<Option<u64>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        let schema = self.buffer[0].schema();
        let merged = arrow::compute::concat_batches(&schema, &self.buffer)?;
        let version = self.inserter.insert(&merged)?;

        self.total_flushed += self.buffered_rows as u64;
        self.flush_count += 1;
        self.buffer.clear();
        self.buffered_rows = 0;
        Ok(Some(version))
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
