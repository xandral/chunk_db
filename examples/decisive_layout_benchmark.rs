//! Focused experiment for the core ChunkDB hypothesis.
//!
//! It compares:
//! - the original fixed rectangular grid;
//! - the current adaptive multi-dimensional grid;
//! - one timestamp-sorted Parquet file with bounded row groups.
//!
//! The output deliberately includes physical-layout metrics in addition to
//! warm latency: small files and candidate compressed bytes are often more
//! informative than a microbenchmark result that fits in the OS cache.

use std::collections::HashSet;
use std::error::Error;
use std::fs::File;
use std::hint::black_box;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use chunk_db::partitioning::{
    cell_row_range, i64_to_ordered_u64, range_bucket_overlaps,
};
use chunk_db::storage::parse_chunk_filename;
use chunk_db::{ChunkDb, Filter, RecordBatch, TableBuilder};
use clap::{Parser, ValueEnum};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::Statistics;
use xxhash_rust::xxh3::xxh3_64;

type AnyResult<T> = Result<T, Box<dyn Error>>;

const TABLE: &str = "events";

#[derive(Parser, Debug)]
#[command(about = "Decisive fixed/adaptive/sorted-Parquet layout experiment")]
struct Args {
    /// `quick` is suitable for a development feedback loop; `full` is the
    /// larger confirmation run and should be executed on an otherwise idle host.
    #[arg(long, value_enum, default_value_t = Profile::Quick)]
    profile: Profile,

    /// CSV destination. Defaults to benchmarks/results/decisive_<profile>.csv.
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Profile {
    Quick,
    Full,
}

#[derive(Clone, Copy)]
struct Parameters {
    rows: usize,
    value_columns: usize,
    sensors: usize,
    hash_buckets: u64,
    target_rows: usize,
    runs: usize,
}

impl Profile {
    fn parameters(self) -> Parameters {
        match self {
            Self::Quick => Parameters {
                rows: 60_000,
                value_columns: 6,
                sensors: 32,
                hash_buckets: 8,
                target_rows: 3_000,
                runs: 3,
            },
            Self::Full => Parameters {
                rows: 500_000,
                value_columns: 32,
                sensors: 512,
                hash_buckets: 32,
                target_rows: 20_000,
                runs: 7,
            },
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Quick => "quick",
            Self::Full => "full",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Distribution {
    Uniform,
    Zipf,
}

impl Distribution {
    fn label(self) -> &'static str {
        match self {
            Self::Uniform => "uniform",
            Self::Zipf => "zipf_1.15",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum IdMode {
    OrderedTimestamp,
    RandomUuid,
}

impl IdMode {
    fn label(self) -> &'static str {
        match self {
            Self::OrderedTimestamp => "ordered_timestamp",
            Self::RandomUuid => "random_uuid",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Query {
    TimeOnePercentNarrow,
    TimeTenPercentWide,
    HotSensorNarrow,
    HotSensorTimeNarrow,
    FullScanWide,
}

impl Query {
    const ALL: [Self; 5] = [
        Self::TimeOnePercentNarrow,
        Self::TimeTenPercentWide,
        Self::HotSensorNarrow,
        Self::HotSensorTimeNarrow,
        Self::FullScanWide,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::TimeOnePercentNarrow => "time_1pct_narrow",
            Self::TimeTenPercentWide => "time_10pct_wide",
            Self::HotSensorNarrow => "hot_sensor_narrow",
            Self::HotSensorTimeNarrow => "hot_sensor_time_1pct_narrow",
            Self::FullScanWide => "full_scan_wide",
        }
    }

    fn has_sensor(self) -> bool {
        matches!(self, Self::HotSensorNarrow | Self::HotSensorTimeNarrow)
    }

    fn time_bounds(self, rows: usize) -> Option<(i64, i64)> {
        let width = match self {
            Self::TimeOnePercentNarrow | Self::HotSensorTimeNarrow => rows / 100,
            Self::TimeTenPercentWide => rows / 10,
            _ => return None,
        }
        .max(1);
        let start = rows * 45 / 100;
        Some((start as i64, (start + width - 1) as i64))
    }

    fn is_wide(self) -> bool {
        matches!(self, Self::TimeTenPercentWide | Self::FullScanWide)
    }

    fn filters(self, rows: usize) -> Vec<Filter> {
        let mut filters = Vec::new();
        if self.has_sensor() {
            filters.push(Filter::eq("sensor", sensor_name(0)));
        }
        if let Some((start, end)) = self.time_bounds(rows) {
            filters.extend(Filter::between("timestamp", start, end));
        }
        filters
    }

    fn projected_columns(self) -> Option<Vec<String>> {
        if self.is_wide() {
            None
        } else {
            Some(vec!["value_0".to_string()])
        }
    }

    fn required_columns(self, include_row_id: bool) -> Option<HashSet<String>> {
        if self.is_wide() {
            return None;
        }
        let mut columns = HashSet::from(["value_0".to_string()]);
        if include_row_id {
            columns.insert("__row_id".to_string());
        }
        if self.has_sensor() {
            columns.insert("sensor".to_string());
        }
        if self.time_bounds(1_000_000).is_some() {
            columns.insert("timestamp".to_string());
        }
        Some(columns)
    }
}

struct Dataset {
    batch: RecordBatch,
    sensor_ids: Vec<usize>,
}

#[derive(Clone, Default)]
struct StorageStats {
    physical_files: usize,
    read_units: usize,
    live_bytes: u64,
    total_disk_bytes: u64,
    row_p50: usize,
    row_p95: usize,
    row_max: usize,
    small_unit_pct: f64,
    row_splits: usize,
    hash_splits: usize,
    range_splits: usize,
    max_row_level: u16,
    max_hash_level: u16,
    max_range_level: u16,
}

#[derive(Clone, Default)]
struct Latency {
    min_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
}

struct BenchRow {
    profile: &'static str,
    distribution: &'static str,
    id_mode: &'static str,
    layout: &'static str,
    query: &'static str,
    result_rows: usize,
    expected_rows: usize,
    setup_ms: f64,
    storage: StorageStats,
    candidate_units: usize,
    candidate_bytes: u64,
    latency: Latency,
}

struct ChunkLayout {
    db: ChunkDb,
    root: PathBuf,
    base_width: u64,
    range_width: Option<u64>,
    mode: IdMode,
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    let args = Args::parse();
    let params = args.profile.parameters();
    let output = args.output.unwrap_or_else(|| {
        PathBuf::from(format!(
            "benchmarks/results/decisive_{}.csv",
            args.profile.label()
        ))
    });
    let workspace = tempfile::tempdir()?;
    let mut results = Vec::new();

    println!(
        "profile={} rows={} values={} sensors={} hash_buckets={} target_rows={} runs={}",
        args.profile.label(),
        params.rows,
        params.value_columns,
        params.sensors,
        params.hash_buckets,
        params.target_rows,
        params.runs
    );
    println!("latencies are warm; candidate_bytes are estimated compressed column bytes");

    for distribution in [Distribution::Uniform, Distribution::Zipf] {
        let data = generate_dataset(params, distribution)?;
        for mode in [IdMode::OrderedTimestamp, IdMode::RandomUuid] {
            println!(
                "\ndataset distribution={} id_mode={}",
                distribution.label(),
                mode.label()
            );
            let case_root = workspace.path().join(format!(
                "{}_{}",
                distribution.label(),
                mode.label()
            ));

            for (layout_name, adaptive) in
                [("fixed_rect", false), ("adaptive_md_grid", true)]
            {
                let root = case_root.join(layout_name);
                let setup_start = Instant::now();
                let layout = setup_chunk_layout(&root, &data.batch, params, mode, adaptive)?;
                let setup_ms = setup_start.elapsed().as_secs_f64() * 1000.0;
                let storage = chunk_storage_stats(&layout, params.target_rows)?;
                println!(
                    "  {:18} setup={:8.1}ms live_files={:4} live={:8.2}MiB disk={:8.2}MiB small={:5.1}% splits[r/h/rg]={}/{}/{} maxlvl={}/{}/{}",
                    layout_name,
                    setup_ms,
                    storage.physical_files,
                    mib(storage.live_bytes),
                    mib(storage.total_disk_bytes),
                    storage.small_unit_pct,
                    storage.row_splits,
                    storage.hash_splits,
                    storage.range_splits,
                    storage.max_row_level,
                    storage.max_hash_level,
                    storage.max_range_level,
                );

                for query in Query::ALL {
                    let expected = expected_rows(&data, query, params.rows);
                    let (actual, latency) =
                        benchmark_chunk_query(&layout.db, query, params, params.runs).await?;
                    if actual != expected {
                        return Err(format!(
                            "{} / {} / {} returned {}, expected {}",
                            distribution.label(),
                            mode.label(),
                            query.label(),
                            actual,
                            expected
                        )
                        .into());
                    }
                    let (candidate_units, candidate_bytes) =
                        chunk_candidates(&layout, query, params)?;
                    println!(
                        "    {:30} p50={:8.3}ms candidates={:4} bytes={:7.2}MiB rows={}",
                        query.label(),
                        latency.p50_ms,
                        candidate_units,
                        mib(candidate_bytes),
                        actual
                    );
                    results.push(BenchRow {
                        profile: args.profile.label(),
                        distribution: distribution.label(),
                        id_mode: mode.label(),
                        layout: layout_name,
                        query: query.label(),
                        result_rows: actual,
                        expected_rows: expected,
                        setup_ms,
                        storage: storage.clone(),
                        candidate_units,
                        candidate_bytes,
                        latency,
                    });
                }
            }

            let parquet_path = case_root.join("sorted.parquet");
            let setup_start = Instant::now();
            write_sorted_parquet(&parquet_path, &data.batch, params.target_rows)?;
            let setup_ms = setup_start.elapsed().as_secs_f64() * 1000.0;
            let storage = parquet_storage_stats(&parquet_path, params.target_rows)?;
            println!(
                "  {:18} setup={:8.1}ms files={} row_groups={:4} bytes={:8.2}MiB small={:5.1}%",
                "sorted_parquet",
                setup_ms,
                storage.physical_files,
                storage.read_units,
                mib(storage.live_bytes),
                storage.small_unit_pct
            );

            for query in Query::ALL {
                let expected = expected_rows(&data, query, params.rows);
                let row_groups = selected_parquet_row_groups(&parquet_path, query, params.rows)?;
                let candidate_bytes = compressed_bytes(
                    &parquet_path,
                    &row_groups,
                    query.required_columns(false).as_ref(),
                )?;
                let (actual, latency) = benchmark_parquet_query(
                    &parquet_path,
                    query,
                    params.rows,
                    params.runs,
                    &row_groups,
                )?;
                if actual != expected {
                    return Err(format!(
                        "sorted Parquet / {} returned {}, expected {}",
                        query.label(),
                        actual,
                        expected
                    )
                    .into());
                }
                println!(
                    "    {:30} p50={:8.3}ms candidates={:4} bytes={:7.2}MiB rows={}",
                    query.label(),
                    latency.p50_ms,
                    row_groups.len(),
                    mib(candidate_bytes),
                    actual
                );
                results.push(BenchRow {
                    profile: args.profile.label(),
                    distribution: distribution.label(),
                    id_mode: mode.label(),
                    layout: "sorted_parquet",
                    query: query.label(),
                    result_rows: actual,
                    expected_rows: expected,
                    setup_ms,
                    storage: storage.clone(),
                    candidate_units: row_groups.len(),
                    candidate_bytes,
                    latency,
                });
            }
        }
    }

    write_csv(&output, &results)?;
    println!("\nCSV written to {}", output.display());
    Ok(())
}

fn generate_dataset(params: Parameters, distribution: Distribution) -> AnyResult<Dataset> {
    let sensor_ids = match distribution {
        Distribution::Uniform => (0..params.rows).map(|i| i % params.sensors).collect(),
        Distribution::Zipf => generate_zipf_ids(params.rows, params.sensors, 1.15),
    };
    let uuids: Vec<String> = (0..params.rows)
        .map(|i| {
            let left = splitmix64(i as u64 ^ 0x9e37_79b9_7f4a_7c15);
            let right = splitmix64(i as u64 ^ 0xd1b5_4a32_d192_ed03);
            format!("{left:016x}{right:016x}")
        })
        .collect();
    let sensor_strings: Vec<String> = sensor_ids.iter().map(|&id| sensor_name(id)).collect();

    let mut columns: Vec<(String, ArrayRef)> = vec![
        ("uuid".to_string(), Arc::new(StringArray::from(uuids)) as ArrayRef),
        (
            "timestamp".to_string(),
            Arc::new(Int64Array::from_iter_values(0..params.rows as i64)) as ArrayRef,
        ),
        (
            "sensor".to_string(),
            Arc::new(StringArray::from(sensor_strings)) as ArrayRef,
        ),
    ];
    for column in 0..params.value_columns {
        columns.push((
            format!("value_{column}"),
            Arc::new(Int64Array::from_iter_values((0..params.rows).map(|row| {
                ((row as u64).wrapping_mul((column as u64 + 3) * 1_000_003) % 1_000_000)
                    as i64
            }))) as ArrayRef,
        ));
    }
    Ok(Dataset {
        batch: RecordBatch::try_from_iter(columns)?,
        sensor_ids,
    })
}

fn generate_zipf_ids(rows: usize, cardinality: usize, exponent: f64) -> Vec<usize> {
    let mut cdf = Vec::with_capacity(cardinality);
    let normalization: f64 = (1..=cardinality)
        .map(|rank| 1.0 / (rank as f64).powf(exponent))
        .sum();
    let mut cumulative = 0.0;
    for rank in 1..=cardinality {
        cumulative += (1.0 / (rank as f64).powf(exponent)) / normalization;
        cdf.push(cumulative);
    }

    (0..rows)
        .map(|row| {
            let random = splitmix64(row as u64 ^ 0xa076_1d64_78bd_642f);
            let unit = random as f64 / u64::MAX as f64;
            let mut low = 0usize;
            let mut high = cdf.len();
            while low < high {
                let middle = (low + high) / 2;
                if cdf[middle] < unit {
                    low = middle + 1;
                } else {
                    high = middle;
                }
            }
            low.min(cardinality - 1)
        })
        .collect()
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn sensor_name(id: usize) -> String {
    format!("sensor-{id:04}")
}

fn setup_chunk_layout(
    root: &Path,
    batch: &RecordBatch,
    params: Parameters,
    mode: IdMode,
    adaptive: bool,
) -> AnyResult<ChunkLayout> {
    std::fs::create_dir_all(root)?;
    let path = root.to_str().ok_or("benchmark path is not valid UTF-8")?;
    let mut db = ChunkDb::open(path)?;
    let row_cells = params.rows.div_ceil(params.target_rows).max(1) as u64;
    let base_width = match (mode, adaptive) {
        (IdMode::OrderedTimestamp, false) => params.target_rows as u64,
        (IdMode::OrderedTimestamp, true) => params.rows.next_power_of_two() as u64,
        (IdMode::RandomUuid, false) => (u64::MAX / row_cells).max(1),
        (IdMode::RandomUuid, true) => u64::MAX,
    };
    let range_width = match mode {
        IdMode::OrderedTimestamp => None,
        IdMode::RandomUuid => Some((params.target_rows * 4) as u64),
    };

    let mut builder = TableBuilder::new(TABLE, path)
        .add_column("uuid", "Utf8", false)
        .add_column("timestamp", "Int64", false)
        .add_column("sensor", "Utf8", false)
        .chunk_rows(base_width)
        .add_hash_dimension("sensor", params.hash_buckets);
    for column in 0..params.value_columns {
        builder = builder.add_column(&format!("value_{column}"), "Int64", false);
    }
    if adaptive {
        builder = builder.max_cell_rows(params.target_rows as u64);
    }
    if let Some(width) = range_width {
        builder = builder.add_range_dimension("timestamp", width);
    }
    builder = match mode {
        IdMode::OrderedTimestamp => builder.with_primary_key_as_row_id("timestamp"),
        IdMode::RandomUuid => builder.with_primary_key_as_row_id("uuid"),
    };
    db.create_table(builder.build())?;
    db.insert(TABLE, batch)?;

    Ok(ChunkLayout {
        db,
        root: root.to_path_buf(),
        base_width,
        range_width,
        mode,
    })
}

async fn benchmark_chunk_query(
    db: &ChunkDb,
    query: Query,
    params: Parameters,
    runs: usize,
) -> AnyResult<(usize, Latency)> {
    black_box(execute_chunk_query(db, query, params).await?);
    let mut times = Vec::with_capacity(runs);
    let mut result = 0;
    for _ in 0..runs {
        let start = Instant::now();
        result = black_box(execute_chunk_query(db, query, params).await?);
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    Ok((result, latency(&mut times)))
}

async fn execute_chunk_query(
    db: &ChunkDb,
    query: Query,
    params: Parameters,
) -> chunk_db::Result<usize> {
    let mut builder = if let Some(columns) = query.projected_columns() {
        let refs: Vec<&str> = columns.iter().map(String::as_str).collect();
        db.select(&refs).from(TABLE)
    } else {
        db.select_all(TABLE)
    };
    for filter in query.filters(params.rows) {
        builder = builder.filter(filter);
    }
    let batches = builder.execute().await?;
    Ok(batches.iter().map(|batch| batch.num_rows()).sum())
}

fn chunk_storage_stats(layout: &ChunkLayout, target_rows: usize) -> AnyResult<StorageStats> {
    let chunks = layout.root.join(TABLE).join("chunks");
    let live_names = layout.db.live_chunk_files(TABLE)?;
    let live_set: HashSet<&str> = live_names.iter().map(String::as_str).collect();
    let mut rows_per_file = Vec::with_capacity(live_names.len());
    let mut live_bytes = 0;
    let mut disk_bytes = 0;

    for entry in std::fs::read_dir(&chunks)? {
        let entry = entry?;
        if entry.path().extension().and_then(|ext| ext.to_str()) != Some("parquet") {
            continue;
        }
        let bytes = entry.metadata()?.len();
        disk_bytes += bytes;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if live_set.contains(name.as_ref()) {
            live_bytes += bytes;
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(entry.path())?)?;
            rows_per_file.push(reader.metadata().file_metadata().num_rows() as usize);
        }
    }
    let mut stats = storage_stats(
        live_names.len(),
        live_names.len(),
        live_bytes,
        disk_bytes,
        rows_per_file,
        target_rows,
    );
    let grid = layout.db.adaptive_grid_stats(TABLE)?;
    stats.row_splits = grid.row_internal_cells;
    stats.hash_splits = grid.hash_splits;
    stats.range_splits = grid.range_splits;
    stats.max_row_level = grid.max_row_level;
    stats.max_hash_level = grid.max_hash_level;
    stats.max_range_level = grid.max_range_level;
    Ok(stats)
}

fn chunk_candidates(
    layout: &ChunkLayout,
    query: Query,
    params: Parameters,
) -> AnyResult<(usize, u64)> {
    let sensor_hash = if query.has_sensor() {
        Some(xxh3_64(sensor_name(0).as_bytes()))
    } else {
        None
    };
    let range_bounds = match (layout.mode, query.time_bounds(params.rows)) {
        (IdMode::RandomUuid, bounds @ Some(_)) => bounds,
        _ => None,
    };
    let ordered_bounds = match (layout.mode, query.time_bounds(params.rows)) {
        (IdMode::OrderedTimestamp, Some((start, end))) => {
            Some((i64_to_ordered_u64(start), i64_to_ordered_u64(end)))
        }
        _ => None,
    };
    let required = query.required_columns(true);
    let chunk_dir = layout.root.join(TABLE).join("chunks");
    let mut files = Vec::new();

    for name in layout.db.live_chunk_files(TABLE)? {
        let (coord, _) = parse_chunk_filename(&name)?;
        if let Some(raw_hash) = sensor_hash {
            let level = coord.hash_levels.first().copied().unwrap_or(0);
            let bucket = chunk_db::catalog::hash_bucket_at_level(
                raw_hash,
                params.hash_buckets,
                level,
            )?;
            if coord.hash_buckets.first().copied() != Some(bucket) {
                continue;
            }
        }
        if let Some((start, end)) = range_bounds {
            let level = coord.range_levels.first().copied().unwrap_or(0);
            if let Some(&bucket) = coord.range_buckets.first() {
                if !range_bucket_overlaps(
                    bucket,
                    level,
                    start,
                    end,
                    layout.range_width.unwrap(),
                )? {
                    continue;
                }
            } else {
                continue;
            }
        }
        if let Some((query_start, query_end)) = ordered_bounds {
            let (start, end) = cell_row_range(coord.row_bucket, layout.base_width, coord.level);
            if start > query_end || (end <= query_start && end != u64::MAX) {
                continue;
            }
        }
        files.push(chunk_dir.join(name));
    }

    let mut bytes = 0;
    for file in &files {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(file)?)?;
        let row_groups: Vec<usize> = (0..reader.metadata().num_row_groups()).collect();
        bytes += compressed_bytes(file, &row_groups, required.as_ref())?;
    }
    Ok((files.len(), bytes))
}

fn write_sorted_parquet(path: &Path, batch: &RecordBatch, target_rows: usize) -> AnyResult<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(target_rows)
        .set_write_batch_size(8_192)
        .build();
    let mut writer = ArrowWriter::try_new(File::create(path)?, batch.schema(), Some(properties))?;
    writer.write(batch)?;
    writer.close()?;
    Ok(())
}

fn parquet_storage_stats(path: &Path, target_rows: usize) -> AnyResult<StorageStats> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let metadata = reader.metadata();
    let rows = metadata
        .row_groups()
        .iter()
        .map(|group| group.num_rows() as usize)
        .collect::<Vec<_>>();
    let bytes = std::fs::metadata(path)?.len();
    Ok(storage_stats(
        1,
        metadata.num_row_groups(),
        bytes,
        bytes,
        rows,
        target_rows,
    ))
}

fn storage_stats(
    physical_files: usize,
    read_units: usize,
    live_bytes: u64,
    total_disk_bytes: u64,
    mut rows: Vec<usize>,
    target_rows: usize,
) -> StorageStats {
    rows.sort_unstable();
    let small = rows.iter().filter(|&&count| count < target_rows / 4).count();
    StorageStats {
        physical_files,
        read_units,
        live_bytes,
        total_disk_bytes,
        row_p50: percentile_usize(&rows, 0.50),
        row_p95: percentile_usize(&rows, 0.95),
        row_max: rows.last().copied().unwrap_or(0),
        small_unit_pct: if rows.is_empty() {
            0.0
        } else {
            small as f64 * 100.0 / rows.len() as f64
        },
        ..StorageStats::default()
    }
}

fn selected_parquet_row_groups(
    path: &Path,
    query: Query,
    rows: usize,
) -> AnyResult<Vec<usize>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let metadata = reader.metadata();
    let schema = reader.parquet_schema();
    let timestamp_index = (0..schema.num_columns())
        .find(|&index| schema.column(index).name() == "timestamp")
        .ok_or("timestamp column missing from Parquet schema")?;
    let Some((query_start, query_end)) = query.time_bounds(rows) else {
        return Ok((0..metadata.num_row_groups()).collect());
    };

    let mut selected = Vec::new();
    for index in 0..metadata.num_row_groups() {
        let Some(Statistics::Int64(stats)) = metadata
            .row_group(index)
            .column(timestamp_index)
            .statistics()
        else {
            selected.push(index);
            continue;
        };
        let Some(&min) = stats.min_opt() else {
            selected.push(index);
            continue;
        };
        let Some(&max) = stats.max_opt() else {
            selected.push(index);
            continue;
        };
        if max >= query_start && min <= query_end {
            selected.push(index);
        }
    }
    Ok(selected)
}

fn benchmark_parquet_query(
    path: &Path,
    query: Query,
    rows: usize,
    runs: usize,
    row_groups: &[usize],
) -> AnyResult<(usize, Latency)> {
    black_box(scan_parquet(path, query, rows, row_groups)?);
    let mut times = Vec::with_capacity(runs);
    let mut result = 0;
    for _ in 0..runs {
        let start = Instant::now();
        result = black_box(scan_parquet(path, query, rows, row_groups)?);
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    Ok((result, latency(&mut times)))
}

fn scan_parquet(path: &Path, query: Query, rows: usize, row_groups: &[usize]) -> AnyResult<usize> {
    if row_groups.is_empty() {
        return Ok(0);
    }
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
        .with_row_groups(row_groups.to_vec());
    if let Some(required) = query.required_columns(false) {
        let indices: Vec<usize> = builder
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| required.contains(field.name()).then_some(index))
            .collect();
        let mask = ProjectionMask::roots(builder.parquet_schema(), indices);
        builder = builder.with_projection(mask);
    }
    let reader = builder.build()?;
    let bounds = query.time_bounds(rows);
    let target_sensor = sensor_name(0);
    let mut count = 0;
    for batch in reader {
        let batch = batch?;
        if bounds.is_none() && !query.has_sensor() {
            count += batch.num_rows();
            continue;
        }
        let timestamps = if bounds.is_some() {
            let index = batch.schema().index_of("timestamp")?;
            Some(
                batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or("timestamp is not Int64")?,
            )
        } else {
            None
        };
        let sensors = if query.has_sensor() {
            let index = batch.schema().index_of("sensor")?;
            Some(
                batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or("sensor is not Utf8")?,
            )
        } else {
            None
        };
        for row in 0..batch.num_rows() {
            let time_matches = bounds
                .map(|(start, end)| {
                    let value = timestamps.unwrap().value(row);
                    value >= start && value <= end
                })
                .unwrap_or(true);
            let sensor_matches = sensors
                .map(|array| array.value(row) == target_sensor)
                .unwrap_or(true);
            if time_matches && sensor_matches {
                count += 1;
            }
        }
    }
    Ok(count)
}

fn compressed_bytes(
    path: &Path,
    row_groups: &[usize],
    required: Option<&HashSet<String>>,
) -> AnyResult<u64> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let metadata = reader.metadata();
    let schema = reader.parquet_schema();
    let mut bytes = 0u64;
    for &row_group in row_groups {
        for column in 0..schema.num_columns() {
            if required
                .map(|names| !names.contains(schema.column(column).name()))
                .unwrap_or(false)
            {
                continue;
            }
            bytes += metadata
                .row_group(row_group)
                .column(column)
                .compressed_size()
                .max(0) as u64;
        }
    }
    Ok(bytes)
}

fn expected_rows(dataset: &Dataset, query: Query, rows: usize) -> usize {
    let bounds = query.time_bounds(rows);
    dataset
        .sensor_ids
        .iter()
        .enumerate()
        .filter(|(row, sensor)| {
            let sensor_matches = !query.has_sensor() || **sensor == 0;
            let time_matches = bounds
                .map(|(start, end)| *row as i64 >= start && *row as i64 <= end)
                .unwrap_or(true);
            sensor_matches && time_matches
        })
        .count()
}

fn latency(times: &mut [f64]) -> Latency {
    times.sort_by(|left, right| left.total_cmp(right));
    Latency {
        min_ms: times.first().copied().unwrap_or(0.0),
        p50_ms: percentile_f64(times, 0.50),
        p95_ms: percentile_f64(times, 0.95),
    }
}

fn percentile_usize(values: &[usize], quantile: f64) -> usize {
    if values.is_empty() {
        return 0;
    }
    let index = ((values.len() as f64 * quantile).ceil() as usize)
        .saturating_sub(1)
        .min(values.len() - 1);
    values[index]
}

fn percentile_f64(values: &[f64], quantile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let index = ((values.len() as f64 * quantile).ceil() as usize)
        .saturating_sub(1)
        .min(values.len() - 1);
    values[index]
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn write_csv(path: &Path, rows: &[BenchRow]) -> AnyResult<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = File::create(path)?;
    writeln!(
        file,
        "profile,distribution,id_mode,layout,query,result_rows,expected_rows,setup_ms,physical_files,read_units,live_bytes,total_disk_bytes,orphan_bytes,row_p50,row_p95,row_max,small_unit_pct,row_splits,hash_splits,range_splits,max_row_level,max_hash_level,max_range_level,candidate_units,candidate_compressed_bytes,min_ms,p50_ms,p95_ms"
    )?;
    for row in rows {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{:.3},{},{},{},{},{},{},{},{},{:.3},{},{},{},{},{},{},{},{},{:.6},{:.6},{:.6}",
            row.profile,
            row.distribution,
            row.id_mode,
            row.layout,
            row.query,
            row.result_rows,
            row.expected_rows,
            row.setup_ms,
            row.storage.physical_files,
            row.storage.read_units,
            row.storage.live_bytes,
            row.storage.total_disk_bytes,
            row.storage.total_disk_bytes.saturating_sub(row.storage.live_bytes),
            row.storage.row_p50,
            row.storage.row_p95,
            row.storage.row_max,
            row.storage.small_unit_pct,
            row.storage.row_splits,
            row.storage.hash_splits,
            row.storage.range_splits,
            row.storage.max_row_level,
            row.storage.max_hash_level,
            row.storage.max_range_level,
            row.candidate_units,
            row.candidate_bytes,
            row.latency.min_ms,
            row.latency.p50_ms,
            row.latency.p95_ms,
        )?;
    }
    Ok(())
}
