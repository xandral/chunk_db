use arrow::record_batch::RecordBatch;
use crate::api::ChunkDb;
use crate::query::filter::{Filter, CompositeFilter};
use crate::Result;

pub struct QueryBuilder<'a> {
    db: &'a ChunkDb,
    table: Option<String>,
    columns: Option<Vec<String>>,
    filters: Vec<CompositeFilter>,
    limit: Option<usize>,
}

impl<'a> QueryBuilder<'a> {
    pub fn new(db: &'a ChunkDb) -> Self {
        Self {
            db,
            table: None,
            columns: None,
            filters: Vec::new(),
            limit: None,
        }
    }

    pub fn select(mut self, columns: &[&str]) -> Self {
        self.columns = Some(columns.iter().map(|s| s.to_string()).collect());
        self
    }

    pub fn select_all(mut self) -> Self {
        self.columns = None;
        self
    }

    pub fn from(mut self, table: &str) -> Self {
        self.table = Some(table.to_string());
        self
    }

    /// Add a filter condition. Accepts:
    /// - `Filter` (single AND condition)
    /// - `Vec<Filter>` (multiple AND conditions, e.g. from `Filter::between()`)
    /// - `CompositeFilter` (OR groups via `Filter::or()`)
    pub fn filter(mut self, f: impl Into<CompositeFilter>) -> Self {
        self.filters.push(f.into());
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Build the final combined filter from all accumulated filters
    fn build_final_filter(&self) -> Option<CompositeFilter> {
        if self.filters.is_empty() {
            None
        } else if self.filters.len() == 1 {
            Some(self.filters[0].clone())
        } else {
            Some(CompositeFilter::And(self.filters.clone()))
        }
    }

    /// Check if any filter contains an OR operation (used for aggregation fast-paths)
    fn has_or(&self) -> bool {
        self.filters.iter().any(|f| f.has_or())
    }

    /// Extract simple AND filters for executor fast-path (only valid when !has_or())
    fn simple_filters(&self) -> Vec<Filter> {
        self.filters.iter().flat_map(|f| f.flatten()).collect()
    }

    pub async fn execute(self) -> Result<Vec<RecordBatch>> {
        let table = self.table.as_ref().ok_or_else(|| {
            crate::ChunkDbError::InvalidQuery("Table name not specified".to_string())
        })?;

        let executor = self.db.create_executor(table)?;
        let final_filter = self.build_final_filter();
        let projection_holder = self.columns;
        let projection = projection_holder.as_deref();

        let batches = if let Some(filter) = final_filter {
            if filter.has_or() {
                executor.execute_composite(filter, projection).await?
            } else {
                let simple_filters = filter.flatten();
                executor.execute(&simple_filters, None, projection).await?
            }
        } else {
            executor.execute(&[], None, projection).await?
        };

        // Apply LIMIT
        if let Some(limit) = self.limit {
            let mut total_rows = 0;
            let mut limited_batches = Vec::new();

            for batch in batches {
                if total_rows >= limit { break; }
                let remaining = limit - total_rows;
                if batch.num_rows() <= remaining {
                    total_rows += batch.num_rows();
                    limited_batches.push(batch);
                } else {
                    limited_batches.push(batch.slice(0, remaining));
                    break;
                }
            }
            Ok(limited_batches)
        } else {
            Ok(batches)
        }
    }

    // --- Aggregation Methods ---

    pub async fn count(self) -> Result<i64> {
        if !self.has_or() {
            let table = self.table.as_ref().ok_or_else(|| crate::ChunkDbError::InvalidQuery("Table not specified".into()))?;
            let executor = self.db.create_executor(table)?;
            return executor.count(&self.simple_filters()).await;
        }
        let batches = self.execute().await?;
        Ok(batches.iter().map(|b| b.num_rows() as i64).sum())
    }

    pub async fn sum(self, column: &str) -> Result<i64> {
        if !self.has_or() {
            let table = self.table.as_ref().ok_or_else(|| crate::ChunkDbError::InvalidQuery("Table not specified".into()))?;
            let executor = self.db.create_executor(table)?;
            return executor.sum(&self.simple_filters(), column).await;
        }

        let mut builder = self;
        builder.columns = Some(vec![column.to_string()]);
        let batches = builder.execute().await?;

        let total: i64 = batches.iter().map(|batch| {
            if let Ok(col_idx) = batch.schema().index_of(column) {
                let col = batch.column(col_idx);
                if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                    arr.values().iter().sum::<i64>()
                } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::UInt64Array>() {
                    arr.values().iter().map(|&v| v as i64).sum::<i64>()
                } else {
                    0i64
                }
            } else {
                0i64
            }
        }).sum();

        Ok(total)
    }

    pub async fn avg(self, column: &str) -> Result<f64> {
        if !self.has_or() {
            let table = self.table.as_ref().ok_or_else(|| crate::ChunkDbError::InvalidQuery("Table not specified".into()))?;
            let executor = self.db.create_executor(table)?;
            return executor.avg(&self.simple_filters(), column).await;
        }

        let mut builder = self;
        builder.columns = Some(vec![column.to_string()]);
        let batches = builder.execute().await?;
        let count: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

        if count == 0 { return Ok(0.0); }

        let sum: i64 = batches.iter().map(|batch| {
            if let Ok(col_idx) = batch.schema().index_of(column) {
                let col = batch.column(col_idx);
                if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                    arr.values().iter().sum::<i64>()
                } else { 0 }
            } else { 0 }
        }).sum();

        Ok(sum as f64 / count as f64)
    }

    pub async fn min(self, column: &str) -> Result<Option<i64>> {
        if !self.has_or() {
            let table = self.table.as_ref().ok_or_else(|| crate::ChunkDbError::InvalidQuery("Table not specified".into()))?;
            let executor = self.db.create_executor(table)?;
            let val = executor.min(&self.simple_filters(), column).await?;
            return Ok(if val == i64::MAX { None } else { Some(val) });
        }

        let mut builder = self;
        builder.columns = Some(vec![column.to_string()]);
        let batches = builder.execute().await?;

        let min_val = batches.iter()
            .filter_map(|batch| {
                batch.schema().index_of(column).ok()
                    .and_then(|col_idx| {
                        batch.column(col_idx).as_any().downcast_ref::<arrow::array::Int64Array>()
                            .and_then(|arr| arr.values().iter().min().copied())
                    })
            })
            .min();

        Ok(min_val)
    }

    pub async fn max(self, column: &str) -> Result<Option<i64>> {
        if !self.has_or() {
            let table = self.table.as_ref().ok_or_else(|| crate::ChunkDbError::InvalidQuery("Table not specified".into()))?;
            let executor = self.db.create_executor(table)?;
            let val = executor.max(&self.simple_filters(), column).await?;
            return Ok(if val == i64::MIN { None } else { Some(val) });
        }

        let mut builder = self;
        builder.columns = Some(vec![column.to_string()]);
        let batches = builder.execute().await?;

        let max_val = batches.iter()
            .filter_map(|batch| {
                batch.schema().index_of(column).ok()
                    .and_then(|col_idx| {
                        batch.column(col_idx).as_any().downcast_ref::<arrow::array::Int64Array>()
                            .and_then(|arr| arr.values().iter().max().copied())
                    })
            })
            .max();

        Ok(max_val)
    }
}
