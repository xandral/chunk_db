//! Explicit Array Playground
//!
//! Run with: cargo run --example playground

use chunk_db::{ChunkDb, TableBuilder, Filter, RecordBatch};
use arrow::array::{ArrayRef, Int64Array, StringArray, Float64Array};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", "=".repeat(70));
    println!("  ChunkDB Playground - EXPLICIT DATA");
    println!("{}", "=".repeat(70));

    let db_path = "./playground_db";

    // 1. Clean slate (always start fresh)
    if std::path::Path::new(db_path).exists() {
        std::fs::remove_dir_all(db_path)?;
    }

    let mut db = ChunkDb::open(db_path)?;

    // -------------------------------------------------------------------------
    // TABLE DEFINITION
    // -------------------------------------------------------------------------
    let config = TableBuilder::new("metrics", db_path)
        .add_column("ts", "Int64", false)       // Timestamp (ID)
        .add_column("host", "Utf8", false)      // Partition Key
        .add_column("service", "Utf8", false)   // Service Name
        .add_column("cpu", "Float64", true)     // Metric value
        .chunk_rows(5)                          // Split into 2 chunks (10 rows total)
        .add_hash_dimension("host", 2)
        .with_primary_key_as_row_id("ts")
        .build();

    db.create_table(config)?;

    // -------------------------------------------------------------------------
    // EXPLICIT DATA (10 ROWS)
    // -------------------------------------------------------------------------
    
    let ts_data = vec![
        100, 101, 102, 103, 104,  // First 5
        105, 106, 107, 108, 109   // Second 5
    ];

    let host_data = vec![
        "host-a", "host-a", "host-b", "host-b", "host-a", // 3x host-a, 2x host-b
        "host-c", "host-c", "host-a", "host-b", "host-c"  // mixed
    ];

    let service_data = vec![
        "db",     "db",     "web",    "api",    "web",
        "db",     "api",    "db",     "web",    "api"
    ];

    let cpu_data = vec![
        10,     20,     30,     40,     50,     // Ascending
        15,     25,     88,     35,     12
    ];

    // Build batch
    let batch = RecordBatch::try_from_iter(vec![
        ("ts", Arc::new(Int64Array::from(ts_data)) as ArrayRef),
        ("host", Arc::new(StringArray::from(host_data)) as ArrayRef),
        ("service", Arc::new(StringArray::from(service_data)) as ArrayRef),
        ("cpu", Arc::new(Int64Array::from(cpu_data)) as ArrayRef),
    ])?;

    println!("Inserting 10 explicit rows...");
    db.insert("metrics", &batch)?;

    let data = db.select(&["ts", "cpu"]).from("metrics").filter(Filter::eq("cpu", 40i64)).execute().await?;
    arrow::util::pretty::print_batches(&data)?;


    // -------------------------------------------------------------------------
    // VERIFICATION
    // -------------------------------------------------------------------------
    // println!("\n--- DATA VERIFICATION ---");

    // // TEST 1: Exact filter on Host
    // // Manual count of 'host-a' in data above:
    // // Indices: 0, 1, 4, 7 -> Total 4 rows
    // let count = db.select_all("metrics")
    //     .filter(Filter::eq("host", "host-a"))
    //     .count().await?;
    // println!("1. Rows where host='host-a' (EXPECT 4): {}", count);
    // assert_eq!(count, 4);

    // // TEST 2: Combined filter (host-a AND db)
    // // Indices: 0 (host-a, db), 1 (host-a, db), 7 (host-a, db) -> Total 3 rows
    // let count = db.select_all("metrics")
    //     .filter(Filter::eq("host", "host-a"))
    //     .filter(Filter::eq("service", "db"))
    //     .count().await?;
    // println!("2. Rows where host='host-a' AND service='db' (EXPECT 3): {}", count);
    // assert_eq!(count, 3);

    // // TEST 3: Range CPU > 40
    // // Values > 40: 50, 88 -> Total 2 rows
    // let count = db.select_all("metrics")
    //     .filter(Filter::gt("cpu", 40))
    //     .count().await?;
    // println!("3. Rows where cpu > 40.0 (EXPECT 3): {}", count);
    // assert_eq!(count, 3);

    // // TEST 4: Specific select
    // println!("\n4. Fetching host-b rows (Visual check):");
    // let results = db.select(&["ts", "host", "cpu"])
    //     .from("metrics")
    //     .filter(Filter::eq("host", "host-b"))
    //     .execute().await?;
    
    // for b in results {
    //     arrow::util::pretty::print_batches(&[b])?;
    // }
    // // You should see only host-b rows with timestamps:
    // // 102 (30), 103 (40), 108 (35)

    Ok(())
}