use arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, StringArray,
    TimestampMicrosecondArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Int16Type, Int32Type, Int8Type, Schema,
    UInt16Type, UInt32Type, UInt8Type,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::api::database::{hot_buffer_key, patch_key};
use crate::catalog::{HashRegistry, VersionCatalog};
use crate::concurrency::CompactionLock;
use crate::config::table_config::TableConfig;
use crate::partitioning::{
    bucket_at_level, cell_is_splittable, i64_to_ordered_u64, range_level_is_splittable,
    is_immediate_child, refine_cell, ColumnGroupMapper, DimensionMap, LevelMap, SplitAxis,
};
use crate::query::chunk_merger::{vertical_join, RowKey};
use crate::storage::{
    chunk_path, write_table_parquet, CellCoordinate, ChunkCache, ChunkCoordinate,
};
use crate::write::patch_apply::{apply_patches, filter_batch, project_patches_to_schema};
use crate::write::patch_log::{PatchLog, PatchOp};
use crate::Result;
use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Groups rows by their chunk coordinates
struct ChunkGrouper {
    /// Map from coordinate to row indices
    groups: HashMap<ChunkCoordinate, Vec<usize>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdaptiveSplitAxis {
    Local(SplitAxis),
    Row,
}

#[derive(Debug, Clone, Copy)]
struct SplitCandidate {
    axis: AdaptiveSplitAxis,
    /// Number of binary refinements before this data actually occupies both
    /// children. Empty intermediate children are legal and sometimes
    /// unavoidable when values sit near a coarse-cell boundary.
    levels_until_partition: u16,
    max_child_rows: usize,
    imbalance: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RebalanceResult {
    pub cells_merged: usize,
    pub files_rewritten: usize,
    pub rows_rewritten: usize,
}

impl ChunkGrouper {
    fn new() -> Self {
        Self { groups: HashMap::new() }
    }

    fn add_row(&mut self, coord: ChunkCoordinate, row_idx: usize) {
        self.groups.entry(coord).or_default().push(row_idx);
    }
}

/// Batch inserter for a table
pub struct BatchInserter {
    config: TableConfig,
    catalog: Arc<VersionCatalog>,
    column_mapper: ColumnGroupMapper,
    base_path: PathBuf,
    level_map: Arc<LevelMap>,
    dimension_map: Arc<DimensionMap>,
    patch_log: Arc<PatchLog>,
    chunk_cache: Arc<ChunkCache>,
    compaction_lock: Arc<CompactionLock>,
    /// Shared per-table lock protecting the read/merge/write/catalog sequence.
    write_lock: Arc<Mutex<()>>,
}

impl BatchInserter {
    pub fn new(
        config: TableConfig,
        catalog: Arc<VersionCatalog>,
        catalog_db: Arc<sled::Db>,
        level_map: Arc<LevelMap>,
        dimension_map: Arc<DimensionMap>,
        patch_log: Arc<PatchLog>,
        chunk_cache: Arc<ChunkCache>,
        compaction_lock: Arc<CompactionLock>,
    ) -> Self {
        Self::new_with_write_lock(
            config,
            catalog,
            catalog_db,
            level_map,
            dimension_map,
            patch_log,
            chunk_cache,
            compaction_lock,
            Arc::new(Mutex::new(())),
        )
    }

    pub(crate) fn new_with_write_lock(
        config: TableConfig,
        catalog: Arc<VersionCatalog>,
        _catalog_db: Arc<sled::Db>,
        level_map: Arc<LevelMap>,
        dimension_map: Arc<DimensionMap>,
        patch_log: Arc<PatchLog>,
        chunk_cache: Arc<ChunkCache>,
        compaction_lock: Arc<CompactionLock>,
        write_lock: Arc<Mutex<()>>,
    ) -> Self {
        let column_mapper = ColumnGroupMapper::new(&config);
        let base_path = PathBuf::from(&config.storage.base_path);

        Self {
            config,
            catalog,
            column_mapper,
            base_path,
            level_map,
            dimension_map,
            patch_log,
            chunk_cache,
            compaction_lock,
            write_lock,
        }
    }

    /// Insert a batch of data
    ///
    /// The batch should include a `__row_id` column (UInt64) for row identification.
    /// If not present, sequential IDs starting from 0 will be assigned.
    pub fn insert(&self, batch: &RecordBatch) -> Result<u64> {
        // Merge-on-write is a read-modify-write operation. Serialize it per
        // table until the catalog offers a coordinate-level compare-and-swap.
        let _write_guard = self.write_lock.lock().unwrap();
        let version = self.catalog.next_transaction_id()?;

        // 1. Calculate chunk coordinates for each row
        let num_rows = batch.num_rows();
        let mut grouper = ChunkGrouper::new();

        // Get or generate row IDs
        let row_ids = self.get_or_generate_row_ids(batch, num_rows)?;

        // Keep raw partition values: the local dimension map may route
        // different cells at different hash/range refinement levels.
        let raw_hashes_per_dimension = self.calculate_raw_hashes(batch)?;

        let range_values_per_dimension = self.calculate_range_values(batch)?;
        let hash_base_buckets: Vec<u64> = self.config.partitioning.hash_dimensions
            .iter()
            .map(|dimension| dimension.num_buckets)
            .collect();
        let range_chunk_sizes: Vec<u64> = self.config.partitioning.range_dimensions
            .iter()
            .map(|dimension| dimension.chunk_size)
            .collect();

        // Update range dimension statistics
        self.update_range_stats(batch)?;

        // Group rows by coordinate (for each column group).
        // The row cell comes from the level map: level 0 by formula unless the
        // cell has been split, in which case routing descends to the leaf.
        // One routing guard for the whole batch (not a lock per row).
        let router = self.level_map.routing();
        let dimension_router = self.dimension_map.routing();
        for row_idx in 0..num_rows {
            let row_id = row_ids[row_idx];
            let (level, row_bucket_idx) = router.route(row_id);

            let raw_hashes: Vec<u64> = raw_hashes_per_dimension.iter()
                .map(|dimension| dimension[row_idx])
                .collect();

            let range_values: Vec<i64> = range_values_per_dimension.iter()
                .map(|dimension| dimension[row_idx])
                .collect();

            let cell = dimension_router.route(
                level,
                row_bucket_idx,
                &raw_hashes,
                &hash_base_buckets,
                &range_values,
                &range_chunk_sizes,
            )?;

            // Create coordinate for each column group
            for col_group in 0..self.column_mapper.num_groups() {
                let coord = cell.with_col_group(col_group);
                grouper.add_row(coord, row_idx);
            }
        }
        // Release the read lock before any split: mark_refined takes the
        // write lock on the same RwLock and would deadlock on this thread.
        drop(dimension_router);
        drop(router);

        // 2. Write chunks (with merge-on-write to prevent row loss)
        // Track the largest physical column-group file in every logical cell.
        let mut cell_peak_rows: HashMap<CellCoordinate, usize> = HashMap::new();

        for (coord, row_indices) in grouper.groups {
            // Get columns for this column group
            let group_columns = self.column_mapper.get_columns_in_group(coord.col_group);

            // Build schema for this column group (include __row_id for merging)
            let mut fields: Vec<Field> = vec![
                Field::new("__row_id", DataType::UInt64, false),
            ];

            let mut arrays: Vec<ArrayRef> = vec![
                Arc::new(self.take_indices(&row_ids, &row_indices)) as ArrayRef,
            ];

            let batch_schema = batch.schema();
            for col_name in group_columns {
                let col_idx = batch_schema.index_of(col_name)?;
                let col = batch.column(col_idx);
                let field = batch_schema.field(col_idx);

                fields.push(Field::clone(field));
                arrays.push(self.take_array(col, &row_indices)?);
            }

            let chunk_schema = Arc::new(Schema::new(fields));
            let chunk_batch = RecordBatch::try_new(chunk_schema, arrays)?;

            // Merge with existing data if this coordinate already has a chunk
            let final_batch = if let Some(prev_version) = self.catalog.get_latest_version(&self.config.name, &coord)? {
                let existing_path = chunk_path(&self.base_path, &self.config.name, &coord, prev_version);
                if existing_path.exists() {
                    let existing_batch = Self::read_parquet_file(&existing_path)?;
                    // Concat then deduplicate by __row_id (keep latest)
                    let merged = arrow::compute::concat_batches(&existing_batch.schema(), &[existing_batch, chunk_batch])?;
                    Self::deduplicate_by_row_id(merged)?
                } else {
                    chunk_batch
                }
            } else {
                chunk_batch
            };

            // Write to file
            let path = chunk_path(&self.base_path, &self.config.name, &coord, version);
            write_table_parquet(&path, &final_batch, &self.config)?;

            // Update catalog
            self.catalog.update_version(&self.config.name, &coord, version)?;

            // Merge-on-write rewrote this row cell: a warm cache entry would
            // serve the pre-insert batch (patch-delta logic only tracks
            // patches, not new base files).
            self.chunk_cache.invalidate(&RowKey::from(&coord).cache_key(&self.config.name));

            let cell = coord.cell();
            let peak = cell_peak_rows.entry(cell).or_insert(0);
            *peak = (*peak).max(final_batch.num_rows());
        }

        self.catalog.flush()?;

        // 3. Adaptive grid: split every logical cell whose largest physical
        // column-group file overflowed. The chooser evaluates local hash,
        // local range, and global row candidates from the actual data.
        if let Some(max_cell_rows) = self.config.partitioning.max_cell_rows {
            let mut worklist: Vec<CellCoordinate> = cell_peak_rows.iter()
                .filter(|(_, &rows)| rows as u64 > max_cell_rows)
                .map(|(cell, _)| cell.clone())
                .collect();

            while let Some(cell) = worklist.pop() {
                let children = self.split_adaptive_cell(&cell)?;
                for (child_cell, child_rows) in children {
                    if child_rows as u64 > max_cell_rows {
                        worklist.push(child_cell);
                    }
                }
            }
        }

        Ok(version)
    }

    /// Buffered insert (hot buffer): assign row ids, then record the batch as
    /// an Insert entry in the WAL-backed PatchLog under the table's hot key.
    /// Durable (fsynced) and visible to queries on return; materialized to
    /// Parquet later by `flush_hot`.
    pub fn buffer_insert(&self, batch: &RecordBatch) -> Result<u64> {
        let prepared = self.with_row_id_column(batch)?;
        let tx_id = self.catalog.next_transaction_id()?;
        self.patch_log.record(
            &hot_buffer_key(&self.config.name),
            tx_id,
            PatchOp::Insert(prepared),
        )?;
        Ok(tx_id)
    }

    /// Record an update after proving that its partition keys still route to
    /// the row's current physical leaf. Moving a row between rectangles is a
    /// delete+insert operation; rejecting it here prevents the historical
    /// failure mode where the row became invisible to pruning on its new key.
    pub fn update_rows(&self, batch: &RecordBatch) -> Result<u64> {
        let _write_guard = self.write_lock.lock().unwrap();
        self.validate_row_identity(batch)?;
        self.validate_update_routing(batch)?;

        let tx_id = self.catalog.next_transaction_id()?;
        let row_id_idx = batch.schema().index_of("__row_id")?;
        let row_ids = batch.column(row_id_idx).as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| crate::ChunkDbError::Config(
                "__row_id must be UInt64".to_string(),
            ))?;

        let router = self.level_map.routing();
        let mut by_cell: HashMap<(u16, u64), Vec<usize>> = HashMap::new();
        for index in 0..row_ids.len() {
            by_cell.entry(router.route(row_ids.value(index)))
                .or_default()
                .push(index);
        }
        for ((level, bucket), indices) in by_cell {
            let sub_batch = take_batch_rows(batch, &indices)?;
            self.patch_log.record(
                &patch_key(&self.config.name, level, bucket),
                tx_id,
                PatchOp::Update(sub_batch),
            )?;
        }
        drop(router);
        Ok(tx_id)
    }

    fn validate_row_identity(&self, batch: &RecordBatch) -> Result<()> {
        use crate::config::table_config::RowIdStrategy;

        let row_id_idx = batch.schema().index_of("__row_id")?;
        let row_ids = batch.column(row_id_idx).as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| crate::ChunkDbError::Config(
                "__row_id must be UInt64".to_string(),
            ))?;
        match &self.config.row_id_strategy {
            RowIdStrategy::Snowflake => return Ok(()),
            RowIdStrategy::SingleColumn(column_name) => {
                let column_idx = batch.schema().index_of(column_name)?;
                let column = batch.column(column_idx);
                for index in 0..batch.num_rows() {
                    let expected = if let Some(array) = column.as_any().downcast_ref::<UInt64Array>() {
                        array.value(index)
                    } else if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
                        i64_to_ordered_u64(array.value(index))
                    } else if let Some(array) = column.as_any().downcast_ref::<UInt32Array>() {
                        array.value(index) as u64
                    } else if let Some(array) = column.as_any().downcast_ref::<arrow::array::Int32Array>() {
                        i64_to_ordered_u64(array.value(index) as i64)
                    } else if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                        HashRegistry::raw_hash_string(array.value(index))
                    } else {
                        return Err(crate::ChunkDbError::Config(format!(
                            "row-id source column '{}' has unsupported update type {:?}",
                            column_name,
                            column.data_type()
                        )));
                    };
                    if expected != row_ids.value(index) {
                        return Err(crate::ChunkDbError::Config(format!(
                            "update changes row-id source column '{}' for __row_id {}; use delete_rows + insert",
                            column_name,
                            row_ids.value(index)
                        )));
                    }
                }
            }
            RowIdStrategy::CompositeHash(columns) => {
                for index in 0..batch.num_rows() {
                    let expected = composite_row_id(batch, columns, index)?;
                    if expected != row_ids.value(index) {
                        return Err(crate::ChunkDbError::Config(format!(
                            "update changes a CompositeHash row-id source for __row_id {}; use delete_rows + insert",
                            row_ids.value(index)
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_update_routing(&self, batch: &RecordBatch) -> Result<()> {
        if self.config.partitioning.hash_dimensions.is_empty()
            && self.config.partitioning.range_dimensions.is_empty()
        {
            return Ok(());
        }

        let row_id_idx = batch.schema().index_of("__row_id")?;
        let row_ids = batch.column(row_id_idx).as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| crate::ChunkDbError::Config(
                "__row_id must be UInt64".to_string(),
            ))?;
        let raw_hashes = self.calculate_raw_hashes(batch)?;
        let range_values = self.calculate_range_values(batch)?;
        let hash_base_buckets: Vec<u64> = self.config.partitioning.hash_dimensions
            .iter()
            .map(|dimension| dimension.num_buckets)
            .collect();
        let range_chunk_sizes: Vec<u64> = self.config.partitioning.range_dimensions
            .iter()
            .map(|dimension| dimension.chunk_size)
            .collect();

        let row_router = self.level_map.routing();
        let dimension_router = self.dimension_map.routing();
        let mut desired = HashMap::with_capacity(batch.num_rows());
        let mut target_ids_by_row_cell: HashMap<(u16, u64), std::collections::HashSet<u64>> =
            HashMap::new();
        for index in 0..batch.num_rows() {
            let row_id = row_ids.value(index);
            let row_cell = row_router.route(row_id);
            let raw_hash_row: Vec<_> = raw_hashes.iter()
                .map(|dimension| dimension[index])
                .collect();
            let range_row: Vec<_> = range_values.iter()
                .map(|dimension| dimension[index])
                .collect();
            let cell = dimension_router.route(
                row_cell.0,
                row_cell.1,
                &raw_hash_row,
                &hash_base_buckets,
                &range_row,
                &range_chunk_sizes,
            )?;
            if desired.insert(row_id, cell).is_some() {
                return Err(crate::ChunkDbError::Config(format!(
                    "update batch contains duplicate __row_id {}", row_id
                )));
            }
            target_ids_by_row_cell.entry(row_cell).or_default().insert(row_id);
        }

        let mut actual = HashMap::new();
        for ((level, bucket), target_ids) in target_ids_by_row_cell {
            let chunks = self.catalog.chunks_for_cell(
                &self.config.name,
                level,
                bucket,
            )?;
            // One physical group is sufficient to locate rows. Group zero is
            // present in every valid logical cell and always carries row IDs.
            for (coord, version) in chunks.into_iter().filter(|(coord, _)| coord.col_group == 0) {
                let path = chunk_path(
                    &self.base_path,
                    &self.config.name,
                    &coord,
                    version,
                );
                let stored = Self::read_parquet_file(&path)?;
                let stored_idx = stored.schema().index_of("__row_id")?;
                let stored_ids = stored.column(stored_idx).as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| crate::ChunkDbError::Serialization(
                        "stored __row_id is not UInt64".to_string(),
                    ))?;
                for &row_id in stored_ids.values() {
                    if target_ids.contains(&row_id)
                        && actual.insert(row_id, coord.cell()).is_some()
                    {
                        return Err(crate::ChunkDbError::Serialization(format!(
                            "__row_id {} exists in multiple adaptive cells", row_id
                        )));
                    }
                }
            }
        }
        drop(dimension_router);
        drop(row_router);

        for (&row_id, desired_cell) in &desired {
            if let Some(actual_cell) = actual.get(&row_id) {
                if actual_cell != desired_cell {
                    return Err(crate::ChunkDbError::Config(format!(
                        "update moves __row_id {} from cell {:?} to {:?}; use delete_rows + insert",
                        row_id, actual_cell, desired_cell
                    )));
                }
            }
        }
        Ok(())
    }

    /// Materialize the hot buffer (entries up to the current transaction)
    /// into chunk files through the normal insert path. Crash-safe: rows keep
    /// the __row_id assigned at buffer time, so if we crash between the
    /// insert and the clear, the replayed re-insert is deduplicated by
    /// merge-on-write. Returns the version written (None = buffer empty).
    pub fn flush_hot(&self) -> Result<Option<u64>> {
        let key = hot_buffer_key(&self.config.name);
        let snapshot_tx = self.catalog.current_transaction_id();
        let entries = self.patch_log.get_patches_up_to(&key, snapshot_tx);
        if entries.is_empty() {
            return Ok(None);
        }

        let mut batches = Vec::with_capacity(entries.len());
        for entry in &entries {
            match &entry.op {
                PatchOp::Insert(batch) => batches.push(batch.clone()),
                other => {
                    return Err(crate::ChunkDbError::Config(format!(
                        "hot buffer for table '{}' contains a non-insert entry ({:?}) — \
                         updates/deletes must go through the cell patch keys",
                        self.config.name, other,
                    )));
                }
            }
        }

        let merged = arrow::compute::concat_batches(&batches[0].schema(), &batches)?;
        let version = self.insert(&merged)?;
        self.patch_log.clear_patches_up_to(&key, snapshot_tx)?;
        Ok(Some(version))
    }

    /// Coalesce underfilled local hash/range siblings after deletes have been
    /// compacted. Merges are intentionally absent from the insert hot path to
    /// avoid split/merge oscillation; `ChunkDb::compact` invokes this once the
    /// row-cell patch log is clean, and callers may trigger it explicitly.
    pub fn rebalance_underfilled(&self) -> Result<RebalanceResult> {
        let _write_guard = self.write_lock.lock().unwrap();
        let Some(max_cell_rows) = self.config.partitioning.max_cell_rows else {
            return Ok(RebalanceResult::default());
        };
        let min_cell_rows = (max_cell_rows / 4).max(1) as usize;
        let max_cell_rows = max_cell_rows as usize;
        let hash_base_buckets: Vec<u64> = self.config.partitioning.hash_dimensions
            .iter()
            .map(|dimension| dimension.num_buckets)
            .collect();
        let mut result = RebalanceResult::default();

        loop {
            let all_chunks = self.catalog.all_chunks(&self.config.name)?;
            let mut leaves: HashMap<CellCoordinate, Vec<(ChunkCoordinate, u64)>> = HashMap::new();
            for (coord, version) in all_chunks {
                leaves.entry(coord.cell()).or_default().push((coord, version));
            }

            // Deepest nodes first. After one successful merge we rebuild the
            // view, allowing a newly formed parent to merge again safely.
            let mut internal = self.dimension_map.snapshot();
            internal.sort_by_key(|(cell, _)| {
                std::cmp::Reverse(
                    cell.hash_levels.iter().map(|&v| v as usize).sum::<usize>()
                        + cell.range_levels.iter().map(|&v| v as usize).sum::<usize>()
                )
            });
            let internal_cells: Vec<_> = internal.iter()
                .map(|(cell, _)| cell.clone())
                .collect();

            let mut merged_one = false;
            for (parent, axis) in internal {
                // Both immediate branches must be physical leaves (an absent
                // branch is fine). Never merge one leaf over a sibling branch
                // that has itself already been refined.
                let mut has_internal_child = false;
                for child in &internal_cells {
                    if is_immediate_child(&parent, child, axis, &hash_base_buckets)? {
                        has_internal_child = true;
                        break;
                    }
                }
                if has_internal_child {
                    continue;
                }
                let mut child_cells = Vec::new();
                for child in leaves.keys() {
                    if is_immediate_child(&parent, child, axis, &hash_base_buckets)? {
                        child_cells.push(child.clone());
                    }
                }
                if child_cells.is_empty()
                    || child_cells.len() > 2
                {
                    continue;
                }

                let row_key = patch_key(
                    &self.config.name,
                    parent.row_level,
                    parent.row_bucket,
                );
                if !self.patch_log.get_patches(&row_key).is_empty() {
                    continue;
                }
                let _cell_guard = match self.compaction_lock.try_acquire(&row_key) {
                    Some(guard) => guard,
                    None => continue,
                };

                let mut child_rows = Vec::with_capacity(child_cells.len());
                let mut complete = true;
                for child in &child_cells {
                    let Some(chunks) = leaves.get(child) else {
                        complete = false;
                        break;
                    };
                    let Some((coord, version)) = chunks.iter().min_by_key(|(coord, _)| coord.col_group) else {
                        complete = false;
                        break;
                    };
                    let path = chunk_path(
                        &self.base_path,
                        &self.config.name,
                        coord,
                        *version,
                    );
                    child_rows.push(Self::read_parquet_file(&path)?.num_rows());
                }
                if !complete {
                    continue;
                }
                let combined_rows: usize = child_rows.iter().sum();
                let should_merge = combined_rows <= max_cell_rows
                    && (child_cells.len() == 1
                        || child_rows.iter().all(|&rows| rows <= min_cell_rows));
                if !should_merge {
                    continue;
                }

                self.merge_dimension_children(&parent, &child_cells, &leaves, &mut result)?;
                merged_one = true;
                break;
            }

            if !merged_one {
                break;
            }
        }

        Ok(result)
    }

    fn merge_dimension_children(
        &self,
        parent: &CellCoordinate,
        children: &[CellCoordinate],
        leaves: &HashMap<CellCoordinate, Vec<(ChunkCoordinate, u64)>>,
        result: &mut RebalanceResult,
    ) -> Result<()> {
        let table = &self.config.name;
        let mut by_group: HashMap<u16, Vec<(ChunkCoordinate, u64)>> = HashMap::new();
        let mut child_coords = Vec::new();
        for child in children {
            let chunks = leaves.get(child).ok_or_else(|| {
                crate::ChunkDbError::Serialization(format!(
                    "adaptive merge lost child {:?} from catalog view", child
                ))
            })?;
            for (coord, version) in chunks {
                by_group.entry(coord.col_group)
                    .or_default()
                    .push((coord.clone(), *version));
                child_coords.push(coord.clone());
            }
        }
        if by_group.len() != self.column_mapper.num_groups() as usize {
            return Err(crate::ChunkDbError::Serialization(format!(
                "adaptive merge for {:?} found {} column groups, expected {}",
                parent,
                by_group.len(),
                self.column_mapper.num_groups()
            )));
        }

        let new_version = self.catalog.next_transaction_id()?;
        let mut parent_entries = Vec::with_capacity(by_group.len());
        for (col_group, mut chunks) in by_group {
            chunks.sort_by(|(left, _), (right, _)| left.cmp(right));
            let batches = chunks.iter()
                .map(|(coord, version)| {
                    let path = chunk_path(&self.base_path, table, coord, *version);
                    Self::read_parquet_file(&path)
                })
                .collect::<Result<Vec<_>>>()?;
            let merged = if batches.len() == 1 {
                batches.into_iter().next().unwrap()
            } else {
                let combined = arrow::compute::concat_batches(&batches[0].schema(), &batches)?;
                Self::deduplicate_by_row_id(combined)?
            };
            let parent_coord = parent.with_col_group(col_group);
            let path = chunk_path(&self.base_path, table, &parent_coord, new_version);
            write_table_parquet(&path, &merged, &self.config)?;
            result.files_rewritten += 1;
            result.rows_rewritten += merged.num_rows();
            parent_entries.push((parent_coord, new_version));
        }

        let dimension_snapshot = self.dimension_map.snapshot_without(parent);
        self.catalog.commit_dimension_merge(
            table,
            &child_coords,
            &parent_entries,
            &dimension_snapshot,
        )?;
        self.dimension_map.replace(dimension_snapshot);
        for coord in &child_coords {
            self.chunk_cache.invalidate(&RowKey::from(coord).cache_key(table));
        }
        self.chunk_cache.invalidate(
            &RowKey::from(&parent.with_col_group(0)).cache_key(table),
        );
        result.cells_merged += 1;
        Ok(())
    }

    /// Return `batch` with a `__row_id` column prepended and the remaining
    /// columns ordered per the table schema — the shape hot-buffer batches
    /// carry (all buffered batches must concat at flush time).
    fn with_row_id_column(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let row_ids = self.get_or_generate_row_ids(batch, batch.num_rows())?;
        let table_schema = self.config.arrow_schema();
        let batch_schema = batch.schema();

        let mut fields: Vec<Field> = vec![Field::new("__row_id", DataType::UInt64, false)];
        let mut arrays: Vec<ArrayRef> =
            vec![Arc::new(UInt64Array::from(row_ids)) as ArrayRef];

        for field in table_schema.fields() {
            if field.name() == "__row_id" {
                continue;
            }
            let idx = batch_schema.index_of(field.name())?;
            fields.push(Field::clone(batch_schema.field(idx)));
            arrays.push(batch.column(idx).clone());
        }

        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
    }

    /// Choose and execute one adaptive split for an overflowing logical cell.
    fn split_adaptive_cell(
        &self,
        cell: &CellCoordinate,
    ) -> Result<Vec<(CellCoordinate, usize)>> {
        // Worklists can contain stale parents after another item triggered a
        // global row split. An absent catalog leaf is therefore a no-op.
        let parents = self.catalog.chunks_for_logical_cell(&self.config.name, cell)?;
        if parents.is_empty() || self.dimension_map.axis_for(cell).is_some() {
            return Ok(vec![]);
        }

        let full_batch = self.read_logical_cell(&parents)?;
        if full_batch.num_rows() <= 1 {
            return Ok(vec![]);
        }

        let Some(axis) = self.choose_split_axis(cell, &full_batch)? else {
            // Every available axis is one-sided or at its representable
            // limit. Keeping one oversized file is safer than an infinite
            // chain of empty-child refinements.
            return Ok(vec![]);
        };

        match axis {
            AdaptiveSplitAxis::Local(axis) => self.split_dimension_cell(cell, axis),
            AdaptiveSplitAxis::Row => self.split_row_cell(cell.row_level, cell.row_bucket),
        }
    }

    /// Read and vertically join every physical column group for one logical
    /// cell. Split decisions must see all configured partition dimensions,
    /// regardless of which column group owns them.
    fn read_logical_cell(
        &self,
        chunks: &[(ChunkCoordinate, u64)],
    ) -> Result<RecordBatch> {
        let mut ordered = chunks.to_vec();
        ordered.sort_by_key(|(coord, _)| coord.col_group);
        let batches = ordered.iter()
            .map(|(coord, version)| {
                let path = chunk_path(
                    &self.base_path,
                    &self.config.name,
                    coord,
                    *version,
                );
                if !path.exists() {
                    return Err(crate::ChunkDbError::ChunkNotFound(
                        path.display().to_string(),
                    ));
                }
                Self::read_parquet_file(&path)
            })
            .collect::<Result<Vec<_>>>()?;

        if batches.len() == 1 {
            Ok(batches.into_iter().next().unwrap())
        } else {
            vertical_join(batches)
        }
    }

    /// Evaluate all next-level children from the observed row distribution.
    /// A local candidate is preferred when its largest child is within 10%
    /// of the best row split: it rewrites one rectangle rather than every
    /// rectangle in the row leaf. Otherwise the better-balanced row axis wins.
    fn choose_split_axis(
        &self,
        cell: &CellCoordinate,
        batch: &RecordBatch,
    ) -> Result<Option<AdaptiveSplitAxis>> {
        let raw_hashes = self.calculate_raw_hashes(batch)?;
        let range_values = self.calculate_range_values(batch)?;
        let row_id_idx = batch.schema().index_of("__row_id")?;
        let row_ids = batch.column(row_id_idx).as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| crate::ChunkDbError::Config(
                "__row_id must be UInt64".to_string(),
            ))?;

        let hash_base_buckets: Vec<u64> = self.config.partitioning.hash_dimensions
            .iter()
            .map(|dimension| dimension.num_buckets)
            .collect();
        let range_chunk_sizes: Vec<u64> = self.config.partitioning.range_dimensions
            .iter()
            .map(|dimension| dimension.chunk_size)
            .collect();

        let mut local_candidates = Vec::new();
        for index in 0..cell.hash_levels.len() {
            let axis = SplitAxis::Hash(index);
            if let Some(candidate) = evaluate_candidate(
                AdaptiveSplitAxis::Local(axis),
                cell,
                row_ids,
                &raw_hashes,
                &hash_base_buckets,
                &range_values,
                &range_chunk_sizes,
                self.config.partitioning.chunk_rows,
            )? {
                local_candidates.push(candidate);
            }
        }
        for (index, &level) in cell.range_levels.iter().enumerate() {
            if !range_level_is_splittable(range_chunk_sizes[index], level) {
                continue;
            }
            let axis = SplitAxis::Range(index);
            if let Some(candidate) = evaluate_candidate(
                AdaptiveSplitAxis::Local(axis),
                cell,
                row_ids,
                &raw_hashes,
                &hash_base_buckets,
                &range_values,
                &range_chunk_sizes,
                self.config.partitioning.chunk_rows,
            )? {
                local_candidates.push(candidate);
            }
        }

        let row_candidate = if cell_is_splittable(
            cell.row_bucket,
            self.config.partitioning.chunk_rows,
            cell.row_level,
        ) {
            evaluate_candidate(
                AdaptiveSplitAxis::Row,
                cell,
                row_ids,
                &raw_hashes,
                &hash_base_buckets,
                &range_values,
                &range_chunk_sizes,
                self.config.partitioning.chunk_rows,
            )?
        } else {
            None
        };

        local_candidates.sort_by_key(candidate_sort_key);
        let best_local = local_candidates.first().copied();
        Ok(match (best_local, row_candidate) {
            (None, None) => None,
            (Some(local), None) => Some(local.axis),
            (None, Some(row)) => Some(row.axis),
            (Some(local), Some(row)) => {
                // u128 avoids overflow for very large Arrow batches.
                if (local.max_child_rows as u128) * 100
                    <= (row.max_child_rows as u128) * 110
                {
                    Some(local.axis)
                } else {
                    Some(row.axis)
                }
            }
        })
    }

    /// Split one exact logical cell along a local hash/range axis. Row-cell
    /// patches deliberately stay on their existing key and are not cleared:
    /// queries apply them idempotently to whichever child contains the row,
    /// and the regular row-cell compactor later materializes them everywhere.
    fn split_dimension_cell(
        &self,
        parent_cell: &CellCoordinate,
        axis: SplitAxis,
    ) -> Result<Vec<(CellCoordinate, usize)>> {
        let table = &self.config.name;
        let row_patch_key = patch_key(
            table,
            parent_cell.row_level,
            parent_cell.row_bucket,
        );
        let _cell_guard = match self.compaction_lock.try_acquire(&row_patch_key) {
            Some(guard) => guard,
            None => return Ok(vec![]),
        };

        // Reload under the compaction lock: the chooser's earlier view could
        // have raced a patch compaction that installed newer base files.
        let parents = self.catalog.chunks_for_logical_cell(table, parent_cell)?;
        if parents.is_empty() || self.dimension_map.axis_for(parent_cell).is_some() {
            return Ok(vec![]);
        }
        let full_batch = self.read_logical_cell(&parents)?;
        let assignments = self.child_assignments(
            parent_cell,
            AdaptiveSplitAxis::Local(axis),
            &full_batch,
        )?;
        if assignments.is_empty() {
            return Ok(vec![]);
        }

        let new_version = self.catalog.next_transaction_id()?;
        let mut child_entries = Vec::new();
        let mut child_peak_rows: HashMap<CellCoordinate, usize> = HashMap::new();

        for (parent_coord, parent_version) in &parents {
            let path = chunk_path(&self.base_path, table, parent_coord, *parent_version);
            let parent_batch = Self::read_parquet_file(&path)?;
            for (child_cell, child_batch) in
                partition_batch_by_assignments(&parent_batch, &assignments)?
            {
                let child_coord = child_cell.with_col_group(parent_coord.col_group);
                let child_path = chunk_path(
                    &self.base_path,
                    table,
                    &child_coord,
                    new_version,
                );
                write_table_parquet(&child_path, &child_batch, &self.config)?;

                child_peak_rows
                    .entry(child_cell)
                    .and_modify(|peak| *peak = (*peak).max(child_batch.num_rows()))
                    .or_insert(child_batch.num_rows());
                child_entries.push((child_coord, new_version));
            }
        }

        let parent_coords: Vec<_> = parents.iter().map(|(coord, _)| coord.clone()).collect();
        let dimension_snapshot = self.dimension_map
            .snapshot_with(parent_cell.clone(), axis);
        self.catalog.commit_dimension_split(
            table,
            &parent_coords,
            &child_entries,
            &dimension_snapshot,
        )?;
        self.dimension_map.mark_refined(parent_cell.clone(), axis);

        for coord in &parent_coords {
            self.chunk_cache.invalidate(&RowKey::from(coord).cache_key(table));
        }

        Ok(child_peak_rows.into_iter().collect())
    }

    fn child_assignments(
        &self,
        parent: &CellCoordinate,
        axis: AdaptiveSplitAxis,
        batch: &RecordBatch,
    ) -> Result<HashMap<u64, CellCoordinate>> {
        let raw_hashes = self.calculate_raw_hashes(batch)?;
        let range_values = self.calculate_range_values(batch)?;
        let hash_base_buckets: Vec<u64> = self.config.partitioning.hash_dimensions
            .iter()
            .map(|dimension| dimension.num_buckets)
            .collect();
        let range_chunk_sizes: Vec<u64> = self.config.partitioning.range_dimensions
            .iter()
            .map(|dimension| dimension.chunk_size)
            .collect();
        let row_id_idx = batch.schema().index_of("__row_id")?;
        let row_ids = batch.column(row_id_idx).as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| crate::ChunkDbError::Config(
                "__row_id must be UInt64".to_string(),
            ))?;

        let mut assignments = HashMap::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            let raw_hash_row: Vec<u64> = raw_hashes.iter()
                .map(|dimension| dimension[row_idx])
                .collect();
            let range_row: Vec<i64> = range_values.iter()
                .map(|dimension| dimension[row_idx])
                .collect();
            let child = child_cell_for_row(
                axis,
                parent,
                row_ids.value(row_idx),
                &raw_hash_row,
                &hash_base_buckets,
                &range_row,
                &range_chunk_sizes,
                self.config.partitioning.chunk_rows,
            )?;
            if assignments.insert(row_ids.value(row_idx), child).is_some() {
                return Err(crate::ChunkDbError::Serialization(format!(
                    "duplicate __row_id {} inside logical cell {:?}",
                    row_ids.value(row_idx), parent
                )));
            }
        }
        Ok(assignments)
    }

    /// Split row cell (level, bucket) in half along the row dimension.
    ///
    /// All coordinates sharing the cell (every hash/range/column-group combo)
    /// split together — vertical join groups by RowKey, so the row-axis
    /// partitioning must stay uniform across column groups. Pending patches
    /// are applied first (compact-on-split): children are born clean.
    /// Returns the child cells created with the largest file size of each.
    fn split_row_cell(
        &self,
        level: u16,
        bucket: u64,
    ) -> Result<Vec<(CellCoordinate, usize)>> {
        let table = &self.config.name;
        let base_width = self.config.partitioning.chunk_rows;
        let parent_key = patch_key(table, level, bucket);

        // Mutual exclusion with the compactor on this cell. If it holds the
        // lock, skip: the cell stays oversized and the next insert touching
        // it retries the split.
        let _cell_guard = match self.compaction_lock.try_acquire(&parent_key) {
            Some(guard) => guard,
            None => return Ok(vec![]),
        };

        // Patches up to this snapshot are folded into the children; newer
        // ones are re-routed to the child keys afterwards.
        let snapshot_tx = self.catalog.current_transaction_id();
        let patches = self.patch_log.get_patches_up_to(&parent_key, snapshot_tx);

        let parents = self.catalog.chunks_for_cell(table, level, bucket)?;
        if parents.is_empty() {
            return Ok(vec![]);
        }

        let child_level = level + 1;
        let new_version = self.catalog.next_transaction_id()?;
        let mut child_entries: Vec<(ChunkCoordinate, u64)> = vec![];
        let mut child_peak_rows: HashMap<CellCoordinate, usize> = HashMap::new();

        for (parent_coord, parent_version) in &parents {
            let path = chunk_path(&self.base_path, table, parent_coord, *parent_version);
            if !path.exists() {
                continue;
            }
            let base = Self::read_parquet_file(&path)?;
            let clean = if patches.is_empty() {
                base
            } else {
                // Chunk files hold __row_id + one column group; patch batches
                // carry the full table schema. Project before applying.
                let projected = project_patches_to_schema(&patches, &base.schema())?;
                apply_patches(base, &projected)?
            };

            for (child_bucket, child_batch) in
                partition_by_child_bucket(&clean, base_width, child_level)?
            {
                let child_coord = parent_coord.child(child_bucket);
                let child_path = chunk_path(&self.base_path, table, &child_coord, new_version);
                write_table_parquet(&child_path, &child_batch, &self.config)?;

                let cell = child_coord.cell();
                let peak = child_peak_rows.entry(cell).or_insert(0);
                *peak = (*peak).max(child_batch.num_rows());
                child_entries.push((child_coord, new_version));
            }
        }

        // Atomic commit: children in, parents out, level map persisted. The
        // in-memory map is marked refined only after the commit succeeds — a
        // failed commit must leave routing on the (still live) parent.
        let parent_coords: Vec<ChunkCoordinate> =
            parents.iter().map(|(c, _)| c.clone()).collect();
        let dimension_snapshot = self.dimension_map
            .snapshot_for_row_split(level, bucket);
        let transition_snapshot = self.dimension_map
            .transition_snapshot_for_row_split(level, bucket);
        self.catalog.commit_row_split(
            table,
            &parent_coords,
            &child_entries,
            &self.level_map.snapshot_with((level, bucket)),
            &dimension_snapshot,
        )?;
        self.dimension_map.replace(transition_snapshot);
        // Barrier: takes the level-map write lock, so every writer that
        // routed to the parent under a RoutingGuard has finished recording
        // its patches before this returns.
        self.level_map.mark_refined(level, bucket);
        // Once the row-map barrier has completed, no route can refer to the
        // old row tree. Match the clean snapshot persisted in the catalog.
        self.dimension_map.replace(dimension_snapshot);

        // The parent key is frozen after mark_refined (writers route to the
        // children now), so read → re-route → clear is race-free. The order
        // matters for WAL crash safety: child records are appended before
        // the parent's Clear, so a crash in between at worst replays a patch
        // on both keys — Update/Delete are idempotent, and parent-key
        // leftovers are inert (no chunks exist at the parent cell).
        // Entries <= snapshot_tx are already materialized in the children.
        for entry in self.patch_log.get_patches(&parent_key) {
            if entry.tx_id <= snapshot_tx {
                continue;
            }
            match entry.op {
                PatchOp::Delete(ids) => {
                    let mut by_child: HashMap<u64, Vec<u64>> = HashMap::new();
                    for id in ids {
                        by_child.entry(bucket_at_level(id, base_width, child_level))
                            .or_default().push(id);
                    }
                    for (child_bucket, ids) in by_child {
                        let key = patch_key(table, child_level, child_bucket);
                        self.patch_log.record(&key, entry.tx_id, PatchOp::Delete(ids))?;
                    }
                }
                PatchOp::Update(batch) => {
                    self.reroute_patch_batch(&batch, entry.tx_id, child_level, PatchOp::Update)?;
                }
                PatchOp::Insert(batch) => {
                    self.reroute_patch_batch(&batch, entry.tx_id, child_level, PatchOp::Insert)?;
                }
            }
        }
        self.patch_log.clear_patches(&parent_key)?;

        // Cached batches for the parent RowKeys are now dead.
        for coord in &parent_coords {
            self.chunk_cache.invalidate(&RowKey::from(coord).cache_key(table));
        }

        Ok(child_peak_rows.into_iter().collect())
    }

    /// Re-route one batch-carrying patch to the child cells it belongs to.
    fn reroute_patch_batch(
        &self,
        batch: &RecordBatch,
        tx_id: u64,
        child_level: u16,
        make_op: fn(RecordBatch) -> PatchOp,
    ) -> Result<()> {
        let base_width = self.config.partitioning.chunk_rows;
        for (child_bucket, sub) in partition_by_child_bucket(batch, base_width, child_level)? {
            let key = patch_key(&self.config.name, child_level, child_bucket);
            self.patch_log.record(&key, tx_id, make_op(sub))?;
        }
        Ok(())
    }

    fn get_or_generate_row_ids(&self, batch: &RecordBatch, num_rows: usize) -> Result<Vec<u64>> {
        use crate::config::table_config::RowIdStrategy;

        // Check if user provided __row_id explicitly
        if let Ok(idx) = batch.schema().index_of("__row_id") {
            let col = batch.column(idx);
            let arr = col.as_any().downcast_ref::<UInt64Array>()
                .ok_or_else(|| crate::ChunkDbError::Config(
                    "__row_id must be UInt64".to_string()
                ))?;
            return Ok(arr.values().to_vec());
        }

        // Generate based on strategy
        match &self.config.row_id_strategy {
            RowIdStrategy::Snowflake => {
                // Allocate row IDs atomically from catalog
                let start_id = self.catalog.allocate_row_ids(num_rows as u64)?;
                Ok((start_id..start_id + num_rows as u64).collect())
            }

            RowIdStrategy::SingleColumn(col_name) => {
                // Use single column as row_id (numeric or string types)
                let col_idx = batch.schema().index_of(col_name)?;
                let col = batch.column(col_idx);

                // Try different types
                if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                    Ok(arr.values().to_vec())
                } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    // Order-preserving mapping: negative values → lower buckets
                    Ok(arr.values().iter().map(|&v| i64_to_ordered_u64(v)).collect())
                } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::UInt32Array>() {
                    Ok(arr.values().iter().map(|&v| v as u64).collect())
                } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int32Array>() {
                    Ok(arr.values().iter().map(|&v| i64_to_ordered_u64(v as i64)).collect())
                } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    // Hash string values to u64 (for UUIDs, etc.)
                    Ok(arr.iter()
                        .map(|opt_str| {
                            let s = opt_str.expect("Primary key column contains null value");
                            HashRegistry::raw_hash_string(s)
                        })
                        .collect())
                } else {
                    Err(crate::ChunkDbError::Config(
                        format!("Primary key column '{}' must be Int64, UInt64, Int32, UInt32, or Utf8, got {:?}",
                                col_name, col.data_type())
                    ))
                }
            }

            RowIdStrategy::CompositeHash(col_names) => {
                (0..num_rows)
                    .map(|row_idx| composite_row_id(batch, col_names, row_idx))
                    .collect()
            }
        }
    }

    fn calculate_raw_hashes(&self, batch: &RecordBatch) -> Result<Vec<Vec<u64>>> {
        let num_rows = batch.num_rows();
        let mut result = vec![];

        for dim in &self.config.partitioning.hash_dimensions {
            let col_idx = batch.schema().index_of(&dim.column)?;
            let col = batch.column(col_idx);

            if col.null_count() != 0 {
                return Err(crate::ChunkDbError::Config(format!(
                    "hash dimension '{}' contains {} null values",
                    dim.column,
                    col.null_count()
                )));
            }

            let hashes: Vec<u64> = match col.data_type() {
                DataType::Utf8 => {
                    let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                    (0..num_rows)
                        .map(|row_idx| HashRegistry::raw_hash_string(arr.value(row_idx)))
                        .collect()
                }
                DataType::Int64 => {
                    let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                    (0..num_rows)
                        .map(|row_idx| HashRegistry::raw_hash_numeric(arr.value(row_idx)))
                        .collect()
                }
                DataType::UInt64 => {
                    let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                    (0..num_rows)
                        .map(|row_idx| HashRegistry::raw_hash_numeric(arr.value(row_idx) as i64))
                        .collect()
                }
                DataType::Int32 => hash_integer_array::<Int32Type, _>(col, num_rows, |v| v as i64),
                DataType::UInt32 => hash_integer_array::<UInt32Type, _>(col, num_rows, |v| v as i64),
                DataType::Int16 => hash_integer_array::<Int16Type, _>(col, num_rows, |v| v as i64),
                DataType::UInt16 => hash_integer_array::<UInt16Type, _>(col, num_rows, |v| v as i64),
                DataType::Int8 => hash_integer_array::<Int8Type, _>(col, num_rows, |v| v as i64),
                DataType::UInt8 => hash_integer_array::<UInt8Type, _>(col, num_rows, |v| v as i64),
                DataType::Boolean => {
                    let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                    (0..num_rows)
                        .map(|row_idx| HashRegistry::raw_hash_bool(arr.value(row_idx)))
                        .collect()
                }
                other => {
                    return Err(crate::ChunkDbError::Config(format!(
                        "unsupported hash dimension type {:?} for '{}'",
                        other, dim.column
                    )));
                }
            };

            result.push(hashes);
        }

        Ok(result)
    }

    fn calculate_range_values(&self, batch: &RecordBatch) -> Result<Vec<Vec<i64>>> {
        let num_rows = batch.num_rows();
        let mut result = vec![];

        for dim in &self.config.partitioning.range_dimensions {
            let col_idx = batch.schema().index_of(&dim.column)?;
            let col = batch.column(col_idx);

            if col.null_count() != 0 {
                return Err(crate::ChunkDbError::Config(format!(
                    "range dimension '{}' contains {} null values",
                    dim.column,
                    col.null_count()
                )));
            }

            let values: Vec<i64> = match col.data_type() {
                DataType::Int64 => {
                    let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                    arr.values().to_vec()
                }
                DataType::Timestamp(_, _) => {
                    let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                    (0..num_rows)
                        .map(|i| arr.value(i) / 1_000_000)
                        .collect()
                }
                other => {
                    return Err(crate::ChunkDbError::Config(format!(
                        "unsupported range dimension type {:?} for '{}'",
                        other, dim.column
                    )));
                }
            };

            result.push(values);
        }

        Ok(result)
    }

    /// Read an existing parquet chunk file for merge-on-write
    fn read_parquet_file(path: &PathBuf) -> Result<RecordBatch> {
        let file = std::fs::File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();
        let reader = builder.build()?;
        let batches: Vec<RecordBatch> = reader
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(schema));
        }
        Ok(arrow::compute::concat_batches(&batches[0].schema(), &batches)?)
    }

    /// Deduplicate rows by __row_id, keeping only the last occurrence of each row_id
    /// This implements upsert semantics for merge-on-write
    fn deduplicate_by_row_id(batch: RecordBatch) -> Result<RecordBatch> {
        use std::collections::HashMap;
        use arrow::array::BooleanArray;

        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(batch);
        }

        // Extract __row_id column (always first column)
        let row_id_col = batch.column(0);
        let row_ids = row_id_col.as_any().downcast_ref::<UInt64Array>()
            .ok_or_else(|| crate::ChunkDbError::Config(
                "__row_id must be UInt64".to_string()
            ))?;

        // Track last occurrence index for each row_id
        let mut last_occurrence: HashMap<u64, usize> = HashMap::new();
        for (idx, &row_id) in row_ids.values().iter().enumerate() {
            last_occurrence.insert(row_id, idx);
        }

        // Build filter mask: keep row if it's the last occurrence of its row_id
        let keep_mask: Vec<bool> = (0..num_rows)
            .map(|idx| {
                let row_id = row_ids.value(idx);
                last_occurrence.get(&row_id) == Some(&idx)
            })
            .collect();

        let filter = BooleanArray::from(keep_mask);

        // Filter all columns
        let filtered_columns: Result<Vec<ArrayRef>> = batch.columns()
            .iter()
            .map(|col| Ok(arrow::compute::filter(col.as_ref(), &filter)?))
            .collect();

        Ok(RecordBatch::try_new(batch.schema(), filtered_columns?)?)
    }

    fn take_indices(&self, values: &[u64], indices: &[usize]) -> UInt64Array {
        UInt64Array::from_iter_values(indices.iter().map(|&i| values[i]))
    }

}

fn evaluate_candidate(
    axis: AdaptiveSplitAxis,
    parent: &CellCoordinate,
    row_ids: &UInt64Array,
    raw_hashes: &[Vec<u64>],
    hash_base_buckets: &[u64],
    range_values: &[Vec<i64>],
    range_chunk_sizes: &[u64],
    row_base_width: u64,
) -> Result<Option<SplitCandidate>> {
    // Look through empty intermediate halves. A coarse cell can contain data
    // only in its far edge (notably signed row IDs around 2^63), so rejecting
    // a one-sided *next* level would incorrectly make it permanently
    // oversized. We still materialize one binary level per atomic commit.
    for depth in 1..=64u16 {
        let target_level = match axis {
            AdaptiveSplitAxis::Row => {
                let Some(level) = parent.row_level.checked_add(depth) else {
                    return Ok(None);
                };
                if depth == 1 {
                    if !cell_is_splittable(
                        parent.row_bucket,
                        row_base_width,
                        parent.row_level,
                    ) {
                        return Ok(None);
                    }
                } else {
                    let prior_level = level - 1;
                    let prior_bucket = bucket_at_level(
                        row_ids.value(0),
                        row_base_width,
                        prior_level,
                    );
                    if !cell_is_splittable(prior_bucket, row_base_width, prior_level) {
                        return Ok(None);
                    }
                }
                level
            }
            AdaptiveSplitAxis::Local(SplitAxis::Hash(index)) => {
                let Some(level) = parent.hash_levels[index].checked_add(depth) else {
                    return Ok(None);
                };
                // Probe representability before walking the batch.
                if crate::catalog::hash_bucket_at_level(
                    0,
                    hash_base_buckets[index],
                    level,
                ).is_err() {
                    return Ok(None);
                }
                level
            }
            AdaptiveSplitAxis::Local(SplitAxis::Range(index)) => {
                let Some(level) = parent.range_levels[index].checked_add(depth) else {
                    return Ok(None);
                };
                if !range_level_is_splittable(range_chunk_sizes[index], level - 1) {
                    return Ok(None);
                }
                level
            }
        };

        let mut counts: HashMap<u64, usize> = HashMap::new();
        for row_idx in 0..row_ids.len() {
            let bucket = match axis {
                AdaptiveSplitAxis::Row => bucket_at_level(
                    row_ids.value(row_idx),
                    row_base_width,
                    target_level,
                ),
                AdaptiveSplitAxis::Local(SplitAxis::Hash(index)) => {
                    crate::catalog::hash_bucket_at_level(
                        raw_hashes[index][row_idx],
                        hash_base_buckets[index],
                        target_level,
                    )?
                }
                AdaptiveSplitAxis::Local(SplitAxis::Range(index)) => {
                    crate::partitioning::range_bucket_at_level(
                        range_values[index][row_idx],
                        range_chunk_sizes[index],
                        target_level,
                    )?
                }
            };
            *counts.entry(bucket).or_insert(0) += 1;
        }

        if counts.len() == 1 {
            continue;
        }
        if counts.len() > 2 {
            return Err(crate::ChunkDbError::Serialization(format!(
                "adaptive split {:?} of {:?} produced {} descendants at one binary level",
                axis,
                parent,
                counts.len()
            )));
        }
        let min_child_rows = *counts.values().min().unwrap();
        let max_child_rows = *counts.values().max().unwrap();
        return Ok(Some(SplitCandidate {
            axis,
            levels_until_partition: depth,
            max_child_rows,
            imbalance: max_child_rows - min_child_rows,
        }));
    }

    Ok(None)
}

fn candidate_sort_key(candidate: &SplitCandidate) -> (usize, u16, usize, u8, usize) {
    let (kind, index) = match candidate.axis {
        AdaptiveSplitAxis::Local(SplitAxis::Range(index)) => (0, index),
        AdaptiveSplitAxis::Local(SplitAxis::Hash(index)) => (1, index),
        AdaptiveSplitAxis::Row => (2, 0),
    };
    (
        candidate.max_child_rows,
        candidate.levels_until_partition,
        candidate.imbalance,
        kind,
        index,
    )
}

fn child_cell_for_row(
    axis: AdaptiveSplitAxis,
    parent: &CellCoordinate,
    row_id: u64,
    raw_hashes: &[u64],
    hash_base_buckets: &[u64],
    range_values: &[i64],
    range_chunk_sizes: &[u64],
    row_base_width: u64,
) -> Result<CellCoordinate> {
    match axis {
        AdaptiveSplitAxis::Row => {
            let child_bucket = bucket_at_level(
                row_id,
                row_base_width,
                parent.row_level + 1,
            );
            Ok(parent.row_child(child_bucket))
        }
        AdaptiveSplitAxis::Local(axis) => refine_cell(
            parent,
            axis,
            raw_hashes,
            hash_base_buckets,
            range_values,
            range_chunk_sizes,
        ),
    }
}

fn partition_batch_by_assignments(
    batch: &RecordBatch,
    assignments: &HashMap<u64, CellCoordinate>,
) -> Result<Vec<(CellCoordinate, RecordBatch)>> {
    if batch.num_rows() == 0 {
        return Ok(vec![]);
    }
    let row_id_idx = batch.schema().index_of("__row_id")?;
    let row_ids = batch.column(row_id_idx).as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| crate::ChunkDbError::Config(
            "__row_id must be UInt64".to_string(),
        ))?;

    let mut groups: HashMap<CellCoordinate, Vec<u32>> = HashMap::new();
    for (row_idx, &row_id) in row_ids.values().iter().enumerate() {
        let child = assignments.get(&row_id).ok_or_else(|| {
            crate::ChunkDbError::Serialization(format!(
                "column groups disagree: __row_id {} has no adaptive child assignment",
                row_id
            ))
        })?;
        groups.entry(child.clone()).or_default().push(row_idx as u32);
    }

    let mut groups: Vec<_> = groups.into_iter().collect();
    groups.sort_by(|(left, _), (right, _)| left.cmp(right));
    groups.into_iter()
        .map(|(cell, indices)| {
            let indices = UInt32Array::from(indices);
            let columns = batch.columns().iter()
                .map(|column| {
                    arrow::compute::take(column.as_ref(), &indices, None)
                        .map_err(crate::ChunkDbError::Arrow)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((cell, RecordBatch::try_new(batch.schema(), columns)?))
        })
        .collect()
}

fn take_batch_rows(batch: &RecordBatch, indices: &[usize]) -> Result<RecordBatch> {
    let indices = UInt32Array::from_iter_values(indices.iter().map(|&index| index as u32));
    let columns = batch.columns().iter()
        .map(|column| {
            arrow::compute::take(column.as_ref(), &indices, None)
                .map_err(crate::ChunkDbError::Arrow)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

fn composite_row_id(
    batch: &RecordBatch,
    column_names: &[String],
    row_idx: usize,
) -> Result<u64> {
    let mut hash_input = Vec::new();
    for column_name in column_names {
        let column_idx = batch.schema().index_of(column_name)?;
        let column = batch.column(column_idx);
        if column.is_null(row_idx) {
            return Err(crate::ChunkDbError::Config(format!(
                "CompositeHash source column '{}' contains null", column_name
            )));
        }
        match column.data_type() {
            DataType::Utf8 => {
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                let bytes = array.value(row_idx).as_bytes();
                hash_input.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                hash_input.extend_from_slice(bytes);
            }
            DataType::Int64 => {
                let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                hash_input.extend_from_slice(&8u32.to_le_bytes());
                hash_input.extend_from_slice(&array.value(row_idx).to_le_bytes());
            }
            DataType::UInt64 => {
                let array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                hash_input.extend_from_slice(&8u32.to_le_bytes());
                hash_input.extend_from_slice(&array.value(row_idx).to_le_bytes());
            }
            DataType::Int32 => {
                let array = column.as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .unwrap();
                hash_input.extend_from_slice(&4u32.to_le_bytes());
                hash_input.extend_from_slice(&array.value(row_idx).to_le_bytes());
            }
            DataType::UInt32 => {
                let array = column.as_any().downcast_ref::<UInt32Array>().unwrap();
                hash_input.extend_from_slice(&4u32.to_le_bytes());
                hash_input.extend_from_slice(&array.value(row_idx).to_le_bytes());
            }
            other => {
                return Err(crate::ChunkDbError::Config(format!(
                    "unsupported CompositeHash type {:?} for '{}'",
                    other, column_name
                )));
            }
        }
    }
    Ok(xxhash_rust::xxh3::xxh3_64(&hash_input))
}

fn hash_integer_array<T, F>(
    column: &ArrayRef,
    num_rows: usize,
    to_i64: F,
) -> Vec<u64>
where
    T: ArrowPrimitiveType,
    F: Fn(T::Native) -> i64,
{
    let array = column.as_any()
        .downcast_ref::<arrow::array::PrimitiveArray<T>>()
        .expect("data type and primitive array type must agree");
    (0..num_rows)
        .map(|row_idx| HashRegistry::raw_hash_numeric(to_i64(array.value(row_idx))))
        .collect()
}

/// Partition a batch (must contain a UInt64 `__row_id` column) into its child
/// row cells at `child_level`. Returns only non-empty children — one or two.
fn partition_by_child_bucket(
    batch: &RecordBatch,
    base_width: u64,
    child_level: u16,
) -> Result<Vec<(u64, RecordBatch)>> {
    use arrow::array::BooleanArray;

    if batch.num_rows() == 0 {
        return Ok(vec![]);
    }

    let row_id_idx = batch.schema().index_of("__row_id")?;
    let row_ids = batch.column(row_id_idx).as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| crate::ChunkDbError::Config("__row_id must be UInt64".to_string()))?;

    let buckets: Vec<u64> = row_ids.values().iter()
        .map(|&id| bucket_at_level(id, base_width, child_level))
        .collect();

    // A parent cell has exactly two children (2b and 2b+1), so min/max are
    // the only possible values; anything in between would mean the batch was
    // routed inconsistently.
    let min_bucket = *buckets.iter().min().unwrap();
    let max_bucket = *buckets.iter().max().unwrap();
    debug_assert!(buckets.iter().all(|&b| b == min_bucket || b == max_bucket));

    if min_bucket == max_bucket {
        return Ok(vec![(min_bucket, batch.clone())]);
    }

    let left_mask = BooleanArray::from(
        buckets.iter().map(|&b| b == min_bucket).collect::<Vec<bool>>()
    );
    let right_mask = arrow::compute::not(&left_mask)?;
    Ok(vec![
        (min_bucket, filter_batch(batch, &left_mask)?),
        (max_bucket, filter_batch(batch, &right_mask)?),
    ])
}

impl BatchInserter {
    fn take_array(&self, array: &ArrayRef, indices: &[usize]) -> Result<ArrayRef> {
        let indices_arr = UInt64Array::from_iter_values(indices.iter().map(|&i| i as u64));

        // Use arrow compute take
        Ok(arrow::compute::take(array.as_ref(), &indices_arr, None)?)
    }

    fn update_range_stats(&self, batch: &RecordBatch) -> Result<()> {
        use crate::catalog::RangeDimensionStats;

        for dim in &self.config.partitioning.range_dimensions {
            let col_idx = batch.schema().index_of(&dim.column)?;
            let col = batch.column(col_idx);

            let (batch_min, batch_max) = self.compute_column_min_max(col)?;

            // Get or create stats
            let mut stats = self.catalog
                .get_range_stats(&self.config.name, &dim.column)?
                .unwrap_or_else(|| RangeDimensionStats::new(&dim.column));

            // Update with batch values
            stats.update(batch_min, batch_max, batch.num_rows() as u64);

            // Persist
            self.catalog.update_range_stats(&self.config.name, &dim.column, &stats)?;
        }

        Ok(())
    }

    fn compute_column_min_max(&self, col: &ArrayRef) -> Result<(i64, i64)> {
        use arrow::datatypes::DataType;

        match col.data_type() {
            DataType::Int64 => {
                let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                let min = arrow::compute::min(arr).unwrap_or(i64::MAX);
                let max = arrow::compute::max(arr).unwrap_or(i64::MIN);
                Ok((min, max))
            }
            DataType::Timestamp(_, _) => {
                let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                // Convert to seconds for consistency
                let min = arr.iter().flatten().min().map(|v| v / 1_000_000).unwrap_or(i64::MAX);
                let max = arr.iter().flatten().max().map(|v| v / 1_000_000).unwrap_or(i64::MIN);
                Ok((min, max))
            }
            _ => {
                // Unsupported type for range stats
                Ok((i64::MAX, i64::MIN))
            }
        }
    }
}
