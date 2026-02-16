use super::chunk_coord::ChunkCoordinate;
use crate::{ChunkDbError, Result};
use std::path::PathBuf;

/// Generate chunk filename from coordinate and version
/// Format: chunk_r{row}_c{col}_h{hash1}-{hash2}-..._rg{range1}-{range2}-..._v{version}.parquet
pub fn format_chunk_filename(coord: &ChunkCoordinate, version: u64) -> String {
    let mut parts = vec![
        format!("chunk_r{}", coord.row_bucket),
        format!("c{}", coord.col_group),
    ];

    // Add hash buckets if present
    if !coord.hash_buckets.is_empty() {
        let hash_part = coord.hash_buckets.iter()
            .map(|h| h.to_string())
            .collect::<Vec<_>>()
            .join("-");
        parts.push(format!("h{}", hash_part));
    }

    // Add range buckets if present
    if !coord.range_buckets.is_empty() {
        let range_part = coord.range_buckets.iter()
            .map(|r| r.to_string())
            .collect::<Vec<_>>()
            .join("-");
        parts.push(format!("rg{}", range_part));
    }

    // Always add version at the end
    parts.push(format!("v{}", version));

    format!("{}.parquet", parts.join("_"))
}

/// Parse chunk filename back to coordinate and version
pub fn parse_chunk_filename(filename: &str) -> Result<(ChunkCoordinate, u64)> {
    let name = filename.strip_suffix(".parquet")
        .ok_or_else(|| ChunkDbError::InvalidChunkFilename(filename.to_string()))?;

    let parts: Vec<&str> = name.split('_').collect();

    let mut row_bucket: Option<u64> = None;
    let mut col_group: Option<u16> = None;
    let mut hash_buckets: Vec<u64> = vec![];
    let mut range_buckets: Vec<u64> = vec![];
    let mut version: Option<u64> = None;

    for part in parts {
        if part == "chunk" {
            continue;
        } else if let Some(rg) = part.strip_prefix("rg") {
            // Check "rg" BEFORE "r" to avoid false match
            range_buckets = rg.split('-')
                .map(|s| s.parse().map_err(|_|
                    ChunkDbError::InvalidChunkFilename(filename.to_string())))
                .collect::<Result<Vec<_>>>()?;
        } else if let Some(r) = part.strip_prefix('r') {
            row_bucket = Some(r.parse().map_err(|_|
                ChunkDbError::InvalidChunkFilename(filename.to_string()))?);
        } else if let Some(c) = part.strip_prefix('c') {
            col_group = Some(c.parse().map_err(|_|
                ChunkDbError::InvalidChunkFilename(filename.to_string()))?);
        } else if let Some(h) = part.strip_prefix('h') {
            hash_buckets = h.split('-')
                .map(|s| s.parse().map_err(|_|
                    ChunkDbError::InvalidChunkFilename(filename.to_string())))
                .collect::<Result<Vec<_>>>()?;
        } else if let Some(v) = part.strip_prefix('v') {
            version = Some(v.parse().map_err(|_|
                ChunkDbError::InvalidChunkFilename(filename.to_string()))?);
        } else {
            // Unknown part
            return Err(ChunkDbError::InvalidChunkFilename(
                format!("Unknown part '{}' in filename: {}", part, filename)
            ));
        }
    }

    let coord = ChunkCoordinate {
        row_bucket: row_bucket.ok_or_else(||
            ChunkDbError::InvalidChunkFilename(filename.to_string()))?,
        col_group: col_group.ok_or_else(||
            ChunkDbError::InvalidChunkFilename(filename.to_string()))?,
        hash_buckets,
        range_buckets,
    };

    let ver = version.ok_or_else(||
        ChunkDbError::InvalidChunkFilename(filename.to_string()))?;

    Ok((coord, ver))
}

/// Get full path for a chunk
pub fn chunk_path(base_dir: &PathBuf, table_name: &str, coord: &ChunkCoordinate, version: u64) -> PathBuf {
    base_dir
        .join(table_name)
        .join("chunks")
        .join(format_chunk_filename(coord, version))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_parse_roundtrip() {
        let coord = ChunkCoordinate {
            row_bucket: 5,
            col_group: 2,
            hash_buckets: vec![42, 17],
            range_buckets: vec![100, 200],
        };
        let version = 3;

        let filename = format_chunk_filename(&coord, version);
        assert_eq!(filename, "chunk_r5_c2_h42-17_rg100-200_v3.parquet");

        let (parsed_coord, parsed_version) = parse_chunk_filename(&filename).unwrap();
        assert_eq!(parsed_coord, coord);
        assert_eq!(parsed_version, version);
    }

    #[test]
    fn test_minimal_chunk_name() {
        let coord = ChunkCoordinate {
            row_bucket: 0,
            col_group: 0,
            hash_buckets: vec![],
            range_buckets: vec![],
        };

        let filename = format_chunk_filename(&coord, 1);
        assert_eq!(filename, "chunk_r0_c0_v1.parquet");
    }
}


