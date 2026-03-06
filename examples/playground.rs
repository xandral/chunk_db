//! ChunkDB Playground - Full CRUD + Stream Insert Demo
//!
//! Run with: cargo run --example playground --release

use chunk_db::{ChunkDb, TableBuilder, Filter, RecordBatch};
use chunk_db::write::StreamConfig;
use arrow::array::{ArrayRef, Int64Array, StringArray, UInt64Array};
use std::sync::Arc;

/// Same transform used by BatchInserter for SingleColumn(Int64) row IDs
fn ts_to_row_id(ts: i64) -> u64 {
    (ts as u64) ^ (1u64 << 63)
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn print_batches(label: &str, batches: &[RecordBatch]) {
    println!("\n  {label} ({} rows):", total_rows(batches));
    if batches.iter().all(|b| b.num_rows() == 0) {
        println!("  (empty)");
        return;
    }
    arrow::util::pretty::print_batches(batches).unwrap();
}

fn check(label: &str, expected: usize, actual: usize) {
    let status = if expected == actual { "OK" } else { "FAIL" };
    println!("  [{status}] {label}: expected={expected}, actual={actual}");
    assert_eq!(expected, actual, "{label}");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = "./playground_db";
    if std::path::Path::new(db_path).exists() {
        std::fs::remove_dir_all(db_path)?;
    }

    let mut db = ChunkDb::open(db_path)?;

    // =========================================================================
    // TABLE DEFINITION
    // =========================================================================
    println!("{}", "=".repeat(70));
    println!("  ChunkDB Playground - Full CRUD Demo");
    println!("{}", "=".repeat(70));

    let config = TableBuilder::new("metrics", db_path)
        .add_column("ts", "Int64", false)
        .add_column("host", "Utf8", false)
        .add_column("service", "Utf8", false)
        .add_column("cpu", "Int64", true)
        .chunk_rows(100_000)
        .add_hash_dimension("host", 4)
        .with_primary_key_as_row_id("ts")
        .build();

    db.create_table(config)?;

    // =========================================================================
    // 1. BATCH INSERT (10 rows)
    // =========================================================================
    println!("\n{}", "-".repeat(70));
    println!("  1. BATCH INSERT");
    println!("{}", "-".repeat(70));

    println!("\n  Input data (10 rows):");
    println!("  ts  | host   | service | cpu");
    println!("  ----|--------|---------|----");
    println!("  100 | host-a | db      | 10");
    println!("  101 | host-a | db      | 20");
    println!("  102 | host-b | web     | 30");
    println!("  103 | host-b | api     | 40");
    println!("  104 | host-a | web     | 50");
    println!("  105 | host-c | db      | 15");
    println!("  106 | host-c | api     | 25");
    println!("  107 | host-a | db      | 88");
    println!("  108 | host-b | web     | 35");
    println!("  109 | host-c | api     | 12");

    let batch = RecordBatch::try_from_iter(vec![
        ("ts",      Arc::new(Int64Array::from(vec![100,101,102,103,104,105,106,107,108,109])) as ArrayRef),
        ("host",    Arc::new(StringArray::from(vec!["host-a","host-a","host-b","host-b","host-a","host-c","host-c","host-a","host-b","host-c"])) as ArrayRef),
        ("service", Arc::new(StringArray::from(vec!["db","db","web","api","web","db","api","db","web","api"])) as ArrayRef),
        ("cpu",     Arc::new(Int64Array::from(vec![10,20,30,40,50,15,25,88,35,12])) as ArrayRef),
    ])?;

    let version = db.insert("metrics", &batch)?;
    println!("\n  Inserted at version: {version}");

    // Verify
    let all = db.select_all("metrics").execute().await?;
    check("total rows after insert", 10, total_rows(&all));

    let count_a = db.select_all("metrics").filter(Filter::eq("host", "host-a")).count().await?;
    check("host-a rows", 4, count_a as usize);

    let count_b = db.select_all("metrics").filter(Filter::eq("host", "host-b")).count().await?;
    check("host-b rows", 3, count_b as usize);

    let count_c = db.select_all("metrics").filter(Filter::eq("host", "host-c")).count().await?;
    check("host-c rows", 3, count_c as usize);

    // =========================================================================
    // 2. SELECT QUERIES
    // =========================================================================
    println!("\n{}", "-".repeat(70));
    println!("  2. SELECT QUERIES");
    println!("{}", "-".repeat(70));

    // 2a. Filter by host
    let host_a = db.select(&["ts", "host", "cpu"])
        .from("metrics")
        .filter(Filter::eq("host", "host-a"))
        .execute().await?;
    print_batches("SELECT ts, host, cpu WHERE host='host-a'", &host_a);
    // Expected: ts=100,101,104,107 / cpu=10,20,50,88

    // 2b. Combined filter
    let host_a_db = db.select(&["ts", "cpu"])
        .from("metrics")
        .filter(Filter::eq("host", "host-a"))
        .filter(Filter::eq("service", "db"))
        .execute().await?;
    print_batches("SELECT ts, cpu WHERE host='host-a' AND service='db'", &host_a_db);
    check("host-a AND service=db", 3, total_rows(&host_a_db));
    // Expected: ts=100(cpu=10), 101(cpu=20), 107(cpu=88)

    // 2c. Range filter
    let high_cpu = db.select(&["ts", "host", "cpu"])
        .from("metrics")
        .filter(Filter::gt("cpu", 40i64))
        .execute().await?;
    print_batches("SELECT ts, host, cpu WHERE cpu > 40", &high_cpu);
    check("cpu > 40", 2, total_rows(&high_cpu));
    // Expected: ts=104(cpu=50), ts=107(cpu=88)

    // 2d. Aggregations
    let sum_cpu = db.select_all("metrics").sum("cpu").await?;
    println!("\n  SUM(cpu) = {sum_cpu}  (expected: 10+20+30+40+50+15+25+88+35+12 = 325)");
    assert_eq!(sum_cpu, 325);

    let avg_cpu = db.select_all("metrics").avg("cpu").await?;
    println!("  AVG(cpu) = {avg_cpu:.1}  (expected: 32.5)");
    assert!((avg_cpu - 32.5).abs() < 0.01);

    let min_cpu = db.select_all("metrics").min("cpu").await?;
    let max_cpu = db.select_all("metrics").max("cpu").await?;
    println!("  MIN(cpu) = {:?}  (expected: 10)", min_cpu);
    println!("  MAX(cpu) = {:?}  (expected: 88)", max_cpu);
    assert_eq!(min_cpu, Some(10));
    assert_eq!(max_cpu, Some(88));

    // =========================================================================
    // 3. DELETE ROWS
    // =========================================================================
    println!("\n{}", "-".repeat(70));
    println!("  3. DELETE ROWS");
    println!("{}", "-".repeat(70));

    // Delete ts=103 (host-b, api, cpu=40) and ts=106 (host-c, api, cpu=25)
    let del_ids = vec![ts_to_row_id(103), ts_to_row_id(106)];
    println!("\n  Deleting ts=103 (host-b,api,40) and ts=106 (host-c,api,25)...");
    let del_tx = db.delete_rows("metrics", &del_ids)?;
    println!("  Delete recorded at tx_id: {del_tx}");

    // Verify: 10 - 2 = 8 rows
    let count_after_del = db.select_all("metrics").count().await?;
    check("total rows after delete", 8, count_after_del as usize);

    // host-b should now have 2 rows (was 3, deleted ts=103)
    let count_b_after = db.select_all("metrics").filter(Filter::eq("host", "host-b")).count().await?;
    check("host-b rows after delete", 2, count_b_after as usize);

    // cpu=40 should be gone
    let cpu_40 = db.select_all("metrics").filter(Filter::eq("cpu", 40i64)).count().await?;
    check("rows with cpu=40 after delete", 0, cpu_40 as usize);

    let remaining = db.select(&["ts", "host", "service", "cpu"])
        .from("metrics")
        .execute().await?;
    print_batches("All rows after delete (expect 8)", &remaining);

    // =========================================================================
    // 4. UPDATE ROWS
    // =========================================================================
    println!("\n{}", "-".repeat(70));
    println!("  4. UPDATE ROWS");
    println!("{}", "-".repeat(70));

    // Update ts=107: cpu 88 -> 99, service "db" -> "cache"
    println!("\n  Updating ts=107: cpu 88->99, service 'db'->'cache'...");

    let update_batch = RecordBatch::try_from_iter(vec![
        ("__row_id", Arc::new(UInt64Array::from(vec![ts_to_row_id(107)])) as ArrayRef),
        ("ts",       Arc::new(Int64Array::from(vec![107])) as ArrayRef),
        ("host",     Arc::new(StringArray::from(vec!["host-a"])) as ArrayRef),
        ("service",  Arc::new(StringArray::from(vec!["cache"])) as ArrayRef),
        ("cpu",      Arc::new(Int64Array::from(vec![99])) as ArrayRef),
    ])?;

    let upd_tx = db.update_rows("metrics", &update_batch)?;
    println!("  Update recorded at tx_id: {upd_tx}");

    // Verify: still 8 rows, but cpu=88 is gone, cpu=99 exists
    let count_after_upd = db.select_all("metrics").count().await?;
    check("total rows after update (unchanged)", 8, count_after_upd as usize);

    let cpu_88 = db.select_all("metrics").filter(Filter::eq("cpu", 88i64)).count().await?;
    check("rows with cpu=88 (should be gone)", 0, cpu_88 as usize);

    let cpu_99 = db.select_all("metrics").filter(Filter::eq("cpu", 99i64)).count().await?;
    check("rows with cpu=99 (new value)", 1, cpu_99 as usize);

    let cache_rows = db.select_all("metrics").filter(Filter::eq("service", "cache")).count().await?;
    check("rows with service='cache'", 1, cache_rows as usize);

    let updated_row = db.select(&["ts", "host", "service", "cpu"])
        .from("metrics")
        .filter(Filter::eq("cpu", 99i64))
        .execute().await?;
    print_batches("Updated row (ts=107, cpu=99, service='cache')", &updated_row);

    // New aggregations after delete+update
    // Original: 10+20+30+40+50+15+25+88+35+12 = 325
    // After delete ts=103(40), ts=106(25): 325 - 40 - 25 = 260
    // After update ts=107 cpu 88->99: 260 - 88 + 99 = 271
    let sum_after = db.select_all("metrics").sum("cpu").await?;
    println!("\n  SUM(cpu) after delete+update = {sum_after}  (expected: 271)");
    assert_eq!(sum_after, 271);

    let max_after = db.select_all("metrics").max("cpu").await?;
    println!("  MAX(cpu) after update = {:?}  (expected: 99)", max_after);
    assert_eq!(max_after, Some(99));

    // =========================================================================
    // 5. COMPACTION
    // =========================================================================
    println!("\n{}", "-".repeat(70));
    println!("  5. COMPACTION (materialize patches to disk)");
    println!("{}", "-".repeat(70));

    let result = db.compact("metrics")?;
    println!("\n  Compaction result: {:?}", result);
    println!("  Patches applied: {}", result.patches_applied);
    println!("  Chunks compacted: {}", result.chunks_compacted);

    // Data must be identical after compaction
    let count_post_compact = db.select_all("metrics").count().await?;
    check("rows after compaction", 8, count_post_compact as usize);

    let sum_post_compact = db.select_all("metrics").sum("cpu").await?;
    check("SUM(cpu) after compaction", 271, sum_post_compact as usize);

    let post_compact = db.select(&["ts", "host", "service", "cpu"])
        .from("metrics")
        .execute().await?;
    print_batches("All rows after compaction (should match pre-compaction)", &post_compact);

    // =========================================================================
    // 6. STREAM INSERT
    // =========================================================================
    println!("\n{}", "-".repeat(70));
    println!("  6. STREAM INSERT (buffered micro-batches)");
    println!("{}", "-".repeat(70));

    let mut stream = db.stream_inserter("metrics", StreamConfig { buffer_capacity: 3 })?;

    // Micro-batch 1: 2 rows (below capacity, no flush yet)
    println!("\n  Writing micro-batch 1 (2 rows: ts=200,201)...");
    let mb1 = RecordBatch::try_from_iter(vec![
        ("ts",      Arc::new(Int64Array::from(vec![200, 201])) as ArrayRef),
        ("host",    Arc::new(StringArray::from(vec!["host-d", "host-d"])) as ArrayRef),
        ("service", Arc::new(StringArray::from(vec!["queue", "queue"])) as ArrayRef),
        ("cpu",     Arc::new(Int64Array::from(vec![77, 66])) as ArrayRef),
    ])?;
    let flushed = stream.write(&mb1)?;
    println!("  Flushed? {:?}  Buffered: {}", flushed, stream.buffered_rows());

    // Micro-batch 2: 2 more rows (total 4 >= capacity 3 -> auto-flush)
    println!("  Writing micro-batch 2 (2 rows: ts=202,203)...");
    let mb2 = RecordBatch::try_from_iter(vec![
        ("ts",      Arc::new(Int64Array::from(vec![202, 203])) as ArrayRef),
        ("host",    Arc::new(StringArray::from(vec!["host-d", "host-e"])) as ArrayRef),
        ("service", Arc::new(StringArray::from(vec!["queue", "web"])) as ArrayRef),
        ("cpu",     Arc::new(Int64Array::from(vec![55, 44])) as ArrayRef),
    ])?;
    let flushed = stream.write(&mb2)?;
    println!("  Flushed? {:?}  Buffered: {}", flushed, stream.buffered_rows());

    // Close to flush remaining
    println!("  Closing stream (flush remaining)...");
    let final_tx = stream.close()?;
    println!("  Final flush tx: {:?}", final_tx);

    // Verify: 8 + 4 = 12 rows
    let count_after_stream = db.select_all("metrics").count().await?;
    check("total rows after stream insert", 12, count_after_stream as usize);

    let host_d = db.select_all("metrics").filter(Filter::eq("host", "host-d")).count().await?;
    check("host-d rows", 3, host_d as usize);

    let stream_rows = db.select(&["ts", "host", "service", "cpu"])
        .from("metrics")
        .filter(Filter::eq("host", "host-d"))
        .execute().await?;
    print_batches("Stream-inserted host-d rows", &stream_rows);

    // =========================================================================
    // 7. FINAL STATE
    // =========================================================================
    println!("\n{}", "-".repeat(70));
    println!("  7. FINAL STATE SUMMARY");
    println!("{}", "-".repeat(70));

    let final_all = db.select(&["ts", "host", "service", "cpu"])
        .from("metrics")
        .execute().await?;
    print_batches("Complete dataset (12 rows)", &final_all);

    let final_sum = db.select_all("metrics").sum("cpu").await?;
    // 271 (after delete+update) + 77+66+55+44 (stream) = 513
    println!("\n  Final SUM(cpu) = {final_sum}  (expected: 271 + 242 = 513)");
    assert_eq!(final_sum, 513);

    let final_count = db.select_all("metrics").count().await?;
    println!("  Final COUNT(*) = {final_count}  (expected: 12)");
    assert_eq!(final_count, 12);

    let final_avg = db.select_all("metrics").avg("cpu").await?;
    println!("  Final AVG(cpu) = {final_avg:.2}  (expected: {:.2})", 513.0 / 12.0);

    // =========================================================================
    // DONE
    // =========================================================================
    println!("\n{}", "=".repeat(70));
    println!("  ALL CHECKS PASSED");
    println!("{}", "=".repeat(70));

    // Cleanup
    std::fs::remove_dir_all(db_path)?;

    Ok(())
}
