use super::chunk_coord::ChunkCoordinate;
use crate::{ChunkDbError, Result};
use std::path::PathBuf;

/// Generate chunk filename from coordinate and version
/// Format: `chunk_r{row}[_l{row-level}]_c{col}_h{buckets}[_hl{levels}]`
/// `_rg{buckets}[_rgl{levels}]_v{version}.parquet`.
///
/// Zero-valued level vectors are omitted, so files in an unrefined grid keep
/// the original filename format. The parser treats absent vectors as level 0.
pub fn format_chunk_filename(coord: &ChunkCoordinate, version: u64) -> String {
    let mut parts = vec![format!("chunk_r{}", coord.row_bucket)];

    if coord.level > 0 {
        parts.push(format!("l{}", coord.level));
    }

    parts.push(format!("c{}", coord.col_group));

    // Add hash buckets if present
    if !coord.hash_buckets.is_empty() {
        let hash_part = coord.hash_buckets.iter()
            .map(|h| h.to_string())
            .collect::<Vec<_>>()
            .join("-");
        parts.push(format!("h{}", hash_part));

        if coord.hash_levels.iter().any(|&level| level != 0) {
            let levels = coord.hash_levels.iter()
                .map(|level| level.to_string())
                .collect::<Vec<_>>()
                .join("-");
            parts.push(format!("hl{}", levels));
        }
    }

    // Add range buckets if present
    if !coord.range_buckets.is_empty() {
        let range_part = coord.range_buckets.iter()
            .map(|r| r.to_string())
            .collect::<Vec<_>>()
            .join("-");
        parts.push(format!("rg{}", range_part));

        if coord.range_levels.iter().any(|&level| level != 0) {
            let levels = coord.range_levels.iter()
                .map(|level| level.to_string())
                .collect::<Vec<_>>()
                .join("-");
            parts.push(format!("rgl{}", levels));
        }
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
    let mut level: u16 = 0;
    let mut col_group: Option<u16> = None;
    let mut hash_buckets: Vec<u64> = vec![];
    let mut range_buckets: Vec<u64> = vec![];
    let mut hash_levels: Option<Vec<u16>> = None;
    let mut range_levels: Option<Vec<u16>> = None;
    let mut version: Option<u64> = None;

    for part in parts {
        if part == "chunk" {
            continue;
        } else if let Some(levels) = part.strip_prefix("rgl") {
            range_levels = Some(parse_list(levels, filename)?);
        } else if let Some(levels) = part.strip_prefix("hl") {
            hash_levels = Some(parse_list(levels, filename)?);
        } else if let Some(l) = part.strip_prefix('l') {
            level = l.parse().map_err(|_|
                ChunkDbError::InvalidChunkFilename(filename.to_string()))?;
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

    let hash_levels = hash_levels.unwrap_or_else(|| vec![0; hash_buckets.len()]);
    let range_levels = range_levels.unwrap_or_else(|| vec![0; range_buckets.len()]);
    if hash_levels.len() != hash_buckets.len()
        || range_levels.len() != range_buckets.len()
    {
        return Err(ChunkDbError::InvalidChunkFilename(format!(
            "bucket/level arity mismatch in filename: {}",
            filename
        )));
    }

    let coord = ChunkCoordinate {
        row_bucket: row_bucket.ok_or_else(||
            ChunkDbError::InvalidChunkFilename(filename.to_string()))?,
        level,
        col_group: col_group.ok_or_else(||
            ChunkDbError::InvalidChunkFilename(filename.to_string()))?,
        hash_buckets,
        range_buckets,
        hash_levels,
        range_levels,
    };

    let ver = version.ok_or_else(||
        ChunkDbError::InvalidChunkFilename(filename.to_string()))?;

    Ok((coord, ver))
}

fn parse_list<T>(part: &str, filename: &str) -> Result<Vec<T>>
where
    T: std::str::FromStr,
{
    if part.is_empty() {
        return Err(ChunkDbError::InvalidChunkFilename(filename.to_string()));
    }
    part.split('-')
        .map(|value| {
            value.parse().map_err(|_| {
                ChunkDbError::InvalidChunkFilename(filename.to_string())
            })
        })
        .collect()
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
            level: 0,
            col_group: 2,
            hash_buckets: vec![42, 17],
            range_buckets: vec![100, 200],
            hash_levels: vec![0, 0],
            range_levels: vec![0, 0],
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
            level: 0,
            col_group: 0,
            hash_buckets: vec![],
            range_buckets: vec![],
            hash_levels: vec![],
            range_levels: vec![],
        };

        let filename = format_chunk_filename(&coord, 1);
        assert_eq!(filename, "chunk_r0_c0_v1.parquet");
    }

    #[test]
    fn test_level_roundtrip() {
        let coord = ChunkCoordinate {
            row_bucket: 11,
            level: 3,
            col_group: 1,
            hash_buckets: vec![7],
            range_buckets: vec![4610],
            hash_levels: vec![0],
            range_levels: vec![0],
        };

        let filename = format_chunk_filename(&coord, 2);
        assert_eq!(filename, "chunk_r11_l3_c1_h7_rg4610_v2.parquet");

        let (parsed_coord, parsed_version) = parse_chunk_filename(&filename).unwrap();
        assert_eq!(parsed_coord, coord);
        assert_eq!(parsed_version, 2);
    }

    #[test]
    fn test_legacy_v0_filename_parses_as_level_zero() {
        let (coord, version) = parse_chunk_filename("chunk_r0_c1_h7_rg4610_v2.parquet").unwrap();
        assert_eq!(coord.level, 0);
        assert_eq!(coord.row_bucket, 0);
        assert_eq!(coord.col_group, 1);
        assert_eq!(coord.hash_buckets, vec![7]);
        assert_eq!(coord.range_buckets, vec![4610]);
        assert_eq!(coord.hash_levels, vec![0]);
        assert_eq!(coord.range_levels, vec![0]);
        assert_eq!(version, 2);
    }

    #[test]
    fn test_local_dimension_levels_roundtrip() {
        let coord = ChunkCoordinate {
            row_bucket: 8,
            level: 2,
            col_group: 3,
            hash_buckets: vec![17, 4],
            range_buckets: vec![9],
            hash_levels: vec![3, 0],
            range_levels: vec![2],
        };

        let filename = format_chunk_filename(&coord, 11);
        assert_eq!(
            filename,
            "chunk_r8_l2_c3_h17-4_hl3-0_rg9_rgl2_v11.parquet"
        );
        assert_eq!(parse_chunk_filename(&filename).unwrap(), (coord, 11));
    }

    #[test]
    fn test_rejects_bucket_level_arity_mismatch() {
        let result = parse_chunk_filename("chunk_r0_c0_h1-2_hl1_v1.parquet");
        assert!(result.is_err());
    }
}

