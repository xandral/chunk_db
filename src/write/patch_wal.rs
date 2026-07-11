//! Write-ahead log backing the PatchLog.
//!
//! Every PatchLog mutation (record / clear / clear-up-to / drain) is appended
//! here and fsynced before it is applied in memory, so un-compacted updates
//! and deletes survive a crash. On open the log is replayed into the
//! in-memory map and immediately rewritten from the live state (checkpoint),
//! which drops superseded records and bounds file growth across restarts.
//!
//! Record format (all little-endian):
//!
//! ```text
//! record  = [u32 payload_len][u64 xxh3(payload)][payload]
//! payload = [u8 kind][u32 key_len][key bytes][u64 tx_id][body]
//! kind    = 0 Delete    body = bincode Vec<u64>
//!           1 Update    body = Arrow IPC stream (one batch)
//!           2 Insert    body = Arrow IPC stream (one batch)
//!           3 Clear     body = empty, tx_id unused (0)
//!           4 ClearUpTo body = empty, tx_id = max_tx
//! ```
//!
//! Replay stops at the first truncated or checksum-invalid record: a crash
//! can only corrupt the tail, and a partial final record simply never
//! happened (its fsync did not complete, so its caller never got an Ok).

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use arrow::record_batch::RecordBatch;
use xxhash_rust::xxh3::xxh3_64;

use super::patch_log::{PatchEntry, PatchOp};
use crate::{ChunkDbError, Result};

const KIND_DELETE: u8 = 0;
const KIND_UPDATE: u8 = 1;
const KIND_INSERT: u8 = 2;
const KIND_CLEAR: u8 = 3;
const KIND_CLEAR_UP_TO: u8 = 4;

/// A replayed WAL record, in append order.
pub(crate) enum WalRecord {
    Patch { key: Vec<u8>, entry: PatchEntry },
    Clear { key: Vec<u8> },
    ClearUpTo { key: Vec<u8>, max_tx: u64 },
}

pub(crate) struct PatchWal {
    file: File,
    path: PathBuf,
}

impl PatchWal {
    /// Open (creating parent directories if needed) and replay the log.
    pub fn open(path: &Path) -> Result<(Self, Vec<WalRecord>)> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut records = vec![];
        if path.exists() {
            let mut bytes = vec![];
            File::open(path)?.read_to_end(&mut bytes)?;
            records = replay(&bytes);
        }

        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok((Self { file, path: path.to_path_buf() }, records))
    }

    pub fn append_patch(&mut self, key: &[u8], tx_id: u64, op: &PatchOp) -> Result<()> {
        let (kind, body) = match op {
            PatchOp::Delete(ids) => (
                KIND_DELETE,
                bincode::serialize(ids)
                    .map_err(|e| ChunkDbError::Serialization(e.to_string()))?,
            ),
            PatchOp::Update(batch) => (KIND_UPDATE, batch_to_bytes(batch)?),
            PatchOp::Insert(batch) => (KIND_INSERT, batch_to_bytes(batch)?),
        };
        self.append(kind, key, tx_id, &body)
    }

    pub fn append_clear(&mut self, key: &[u8]) -> Result<()> {
        self.append(KIND_CLEAR, key, 0, &[])
    }

    pub fn append_clear_up_to(&mut self, key: &[u8], max_tx: u64) -> Result<()> {
        self.append(KIND_CLEAR_UP_TO, key, max_tx, &[])
    }

    fn append(&mut self, kind: u8, key: &[u8], tx_id: u64, body: &[u8]) -> Result<()> {
        let mut payload = Vec::with_capacity(1 + 4 + key.len() + 8 + body.len());
        payload.push(kind);
        payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
        payload.extend_from_slice(key);
        payload.extend_from_slice(&tx_id.to_le_bytes());
        payload.extend_from_slice(body);

        self.file.write_all(&(payload.len() as u32).to_le_bytes())?;
        self.file.write_all(&xxh3_64(&payload).to_le_bytes())?;
        self.file.write_all(&payload)?;
        self.file.sync_all()?; // the durability point: ack only after fsync
        Ok(())
    }

    /// Checkpoint: atomically replace the log with the live entries only
    /// (temp file + fsync + rename). Called at open after replay, and when
    /// the in-memory log becomes empty.
    pub fn rewrite<'a>(
        &mut self,
        live: impl Iterator<Item = (&'a [u8], &'a PatchEntry)>,
    ) -> Result<()> {
        let tmp_path = self.path.with_extension("wal.tmp");
        {
            let tmp = OpenOptions::new()
                .create(true).write(true).truncate(true)
                .open(&tmp_path)?;
            let mut fresh = PatchWal { file: tmp, path: tmp_path.clone() };
            for (key, entry) in live {
                fresh.append_patch(key, entry.tx_id, &entry.op)?;
            }
            fresh.file.sync_all()?;
        }
        std::fs::rename(&tmp_path, &self.path)?;
        if let Some(parent) = self.path.parent() {
            // Make the rename itself durable.
            File::open(parent)?.sync_all()?;
        }
        self.file = OpenOptions::new().create(true).append(true).open(&self.path)?;
        Ok(())
    }
}

/// Parse records until the end of the buffer or the first invalid record
/// (a crash-truncated tail).
fn replay(bytes: &[u8]) -> Vec<WalRecord> {
    let mut records = vec![];
    let mut pos = 0usize;

    loop {
        let Some(header) = bytes.get(pos..pos + 12) else { break };
        let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
        let checksum = u64::from_le_bytes(header[4..12].try_into().unwrap());
        let Some(payload) = bytes.get(pos + 12..pos + 12 + len) else { break };
        if xxh3_64(payload) != checksum {
            break;
        }
        pos += 12 + len;

        match parse_payload(payload) {
            Some(record) => records.push(record),
            None => break,
        }
    }

    records
}

fn parse_payload(payload: &[u8]) -> Option<WalRecord> {
    let kind = *payload.first()?;
    let key_len = u32::from_le_bytes(payload.get(1..5)?.try_into().ok()?) as usize;
    let key = payload.get(5..5 + key_len)?.to_vec();
    let tx_pos = 5 + key_len;
    let tx_id = u64::from_le_bytes(payload.get(tx_pos..tx_pos + 8)?.try_into().ok()?);
    let body = payload.get(tx_pos + 8..)?;

    let record = match kind {
        KIND_DELETE => WalRecord::Patch {
            key,
            entry: PatchEntry { tx_id, op: PatchOp::Delete(bincode::deserialize(body).ok()?) },
        },
        KIND_UPDATE => WalRecord::Patch {
            key,
            entry: PatchEntry { tx_id, op: PatchOp::Update(batch_from_bytes(body).ok()?) },
        },
        KIND_INSERT => WalRecord::Patch {
            key,
            entry: PatchEntry { tx_id, op: PatchOp::Insert(batch_from_bytes(body).ok()?) },
        },
        KIND_CLEAR => WalRecord::Clear { key },
        KIND_CLEAR_UP_TO => WalRecord::ClearUpTo { key, max_tx: tx_id },
        _ => return None,
    };
    Some(record)
}

fn batch_to_bytes(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, batch.schema().as_ref())?;
    writer.write(batch)?;
    writer.finish()?;
    drop(writer);
    Ok(buf)
}

fn batch_from_bytes(bytes: &[u8]) -> Result<RecordBatch> {
    let mut reader =
        arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    reader
        .next()
        .ok_or_else(|| ChunkDbError::Serialization("empty IPC stream in WAL record".into()))?
        .map_err(Into::into)
}
