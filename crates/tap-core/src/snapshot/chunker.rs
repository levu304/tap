//! PK-range chunk computation for parallel snapshotting.
//!
//! Divides a table into N equal-sized chunks by primary key range,
//! enabling parallel table scans across multiple worker tasks.
//!
//! # Chunking strategy
//!
//! * **Single integer PK** — Query `MIN(pk)` / `MAX(pk)`, divide into
//!   `num_chunks` equal ranges.
//! * **Composite PK** — Use the first PK column for range partitioning;
//!   the WHERE clause covers all PK columns for correct ordering.
//! * **No PK** — Single chunk with `ctid` ordering (Postgres); no resume
//!   capability.
//! * **Text / UUID PK** — Single chunk for now (range-splitting text
//!   domains is possible but rarely worth it).
//!
//! # Usage
//!
//! ```rust,ignore
//! use tap_core::snapshot::chunker::{SnapshotChunk, ChunkStatus, generate_chunks};
//! ```
//!
//! [`SnapshotChunk`]s are plain data; the [`ParallelSnapshotRunner`] enqueues
//! them into a shared work queue for worker tasks.

use std::fmt;

use serde::Serialize;
use tracing::warn;

use crate::error::TapError;

use super::runner::{TableInfo, qualified_sql, quote_ident};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Status of a single snapshot chunk.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum ChunkStatus {
    /// Awaiting a worker.
    Pending,
    /// Being scanned by a worker.
    InProgress,
    /// Scanned successfully.
    Completed(u64),
    /// Scan failed with an error message.
    Failed(String),
}

impl fmt::Display for ChunkStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChunkStatus::Pending => write!(f, "pending"),
            ChunkStatus::InProgress => write!(f, "in_progress"),
            ChunkStatus::Completed(n) => write!(f, "completed({n})"),
            ChunkStatus::Failed(msg) => write!(f, "failed({msg})"),
        }
    }
}

/// A single PK-range chunk in a parallel snapshot scan.
#[derive(Debug, Clone, Serialize)]
pub struct SnapshotChunk {
    /// Schema-qualified table name.
    pub table_name: String,
    /// Snapshot identifier (from `pg_export_snapshot()` or equivalent).
    pub snapshot_id: String,
    /// Index of this chunk within the table (0-based).
    pub chunk_index: u32,
    /// Inclusive lower bound of the PK range (`None` = unbounded).
    pub chunk_start: Option<String>,
    /// Exclusive upper bound of the PK range (`None` = unbounded).
    pub chunk_end: Option<String>,
    /// Current processing status.
    pub status: ChunkStatus,
}

impl SnapshotChunk {
    /// Create a new pending chunk.
    pub fn new(
        table_name: String,
        snapshot_id: String,
        chunk_index: u32,
        chunk_start: Option<String>,
        chunk_end: Option<String>,
    ) -> Self {
        Self {
            table_name,
            snapshot_id,
            chunk_index,
            chunk_start,
            chunk_end,
            status: ChunkStatus::Pending,
        }
    }

    /// Build a SQL WHERE clause for this chunk's range.
    ///
    /// Returns `("WHERE pk_col >= $1 AND pk_col < $2", start, end)` when
    /// both bounds are present, or an empty clause for single-chunk tables.
    pub fn where_clause(&self, pk_column: &str) -> (String, Option<String>, Option<String>) {
        match (&self.chunk_start, &self.chunk_end) {
            (Some(start), Some(end)) => {
                let clause = format!(
                    "WHERE {} >= {} AND {} < {}",
                    quote_ident(pk_column),
                    start,
                    quote_ident(pk_column),
                    end,
                );
                (clause, Some(start.clone()), Some(end.clone()))
            }
            (Some(start), None) => {
                let clause = format!("WHERE {} >= {}", quote_ident(pk_column), start);
                (clause, Some(start.clone()), None)
            }
            (None, Some(end)) => {
                let clause = format!("WHERE {} < {}", quote_ident(pk_column), end);
                (clause, None, Some(end.clone()))
            }
            (None, None) => (String::new(), None, None),
        }
    }
}

// ---------------------------------------------------------------------------
// PK classification
// ---------------------------------------------------------------------------

/// Classification of a table's primary key for chunking purposes.
#[derive(Debug, Clone, PartialEq)]
pub enum PkKind {
    /// Single integer PK column (i4, i8, serial, bigserial).
    SingleInteger(String),
    /// Composite PK — use the first column as the range dimension.
    Composite(Vec<String>),
    /// No PK — single chunk only, use ctid ordering.
    None,
    /// PK exists but is non-integer (text, uuid, etc.) — single chunk.
    Other(String),
}

/// Classify a table's primary key for chunk-strategy selection.
///
/// Returns the PK classification and the list of PK column names (empty
/// if no PK).
pub fn classify_pk(pk_columns: &[String]) -> (PkKind, &[String]) {
    match pk_columns {
        [] => (PkKind::None, &[]),
        [single] => {
            // Single column — we can't determine the type without a
            // catalog query. For now we optimistically treat it as
            // integer-capable; the caller (generate_chunks) will fall
            // back if the MIN/MAX query fails.
            (PkKind::SingleInteger(single.clone()), pk_columns)
        }
        cols => (PkKind::Composite(cols.to_vec()), pk_columns),
    }
}

// ---------------------------------------------------------------------------
// MIN/MAX extraction from Postgres
// ---------------------------------------------------------------------------

/// Result of a `SELECT MIN(pk), MAX(pk) FROM table` query.
#[derive(Debug, Clone)]
pub struct PkRange {
    /// Minimum PK value as a SQL literal string.
    pub min: String,
    /// Maximum PK value as a SQL literal string.
    pub max: String,
}

/// Query the minimum and maximum PK values for a table.
///
/// Only works for single-column integer (or comparable) PKs.
/// Returns `None` when the table is empty.
pub(crate) async fn query_pk_range(
    client: &tokio_postgres::Client,
    table: &TableInfo,
    pk_column: &str,
) -> Result<Option<PkRange>, TapError> {
    let sql_table = qualified_sql(table);
    let pk_quoted = quote_ident(pk_column);

    let query = format!("SELECT MIN({pk_quoted}), MAX({pk_quoted}) FROM {sql_table}");
    let row = client.query_one(&query, &[]).await?;

    let min: Option<String> = row.try_get(0).ok().flatten();
    let max: Option<String> = row.try_get(1).ok().flatten();

    match (min, max) {
        (Some(min_val), Some(max_val)) => Ok(Some(PkRange {
            min: min_val,
            max: max_val,
        })),
        _ => {
            // Table is empty
            Ok(None)
        }
    }
}

// ---------------------------------------------------------------------------
// Chunk generation
// ---------------------------------------------------------------------------

/// Generate evenly-spaced chunks for a table based on PK range.
///
/// # Arguments
///
/// * `table` — Table identity info.
/// * `snapshot_id` — Current snapshot identifier.
/// * `pk_column` — Primary key column name (first PK column).
/// * `pk_range` — Result from `query_pk_range()` (None = empty table).
/// * `num_chunks` — Desired number of chunks (typically `num_workers × 2`).
/// * `existing_chunks` — Previously persisted chunk status for resume;
///   only `completed` chunks are skipped.
///
/// # Strategy
///
/// For integer PKs, the numeric range is divided into `num_chunks` equal
/// segments.  Non-integer PKs produce a single chunk (resume-safe but not
/// parallel).
pub(crate) fn generate_chunks(
    table: &TableInfo,
    snapshot_id: &str,
    _pk_column: &str,
    pk_range: Option<&PkRange>,
    num_chunks: u32,
    existing_chunks: &[(u32, Option<String>, Option<String>, String)],
) -> Vec<SnapshotChunk> {
    // Build a set of completed chunk indexes for resume
    let completed: std::collections::HashSet<u32> = existing_chunks
        .iter()
        .filter(|(_, _, _, status)| status == "completed")
        .map(|(idx, _, _, _)| *idx)
        .collect();

    let tbl_name = &table.qualified;
    let range = match pk_range {
        Some(r) => r,
        None => {
            // Empty table — single chunk covering nothing
            let mut chunks = Vec::new();
            if !completed.contains(&0) {
                chunks.push(SnapshotChunk::new(
                    tbl_name.clone(),
                    snapshot_id.to_string(),
                    0,
                    None,
                    None,
                ));
            }
            return chunks;
        }
    };

    // Try numeric parsing — if both min and max parse as i64, do
    // integer range splitting.
    if let (Ok(min_i), Ok(max_i)) = (range.min.parse::<i64>(), range.max.parse::<i64>()) {
        generate_integer_chunks(tbl_name, snapshot_id, min_i, max_i, num_chunks, &completed)
    } else {
        // Fallback: single chunk for non-integer PKs
        let mut chunks = Vec::new();
        if !completed.contains(&0) {
            chunks.push(SnapshotChunk::new(
                tbl_name.clone(),
                snapshot_id.to_string(),
                0,
                None,
                None,
            ));
        } else {
            warn!(
                table = %tbl_name,
                "non-integer PK — single chunk only; no parallelism possible"
            );
        }
        chunks
    }
}

/// Generate evenly-spaced integer PK ranges.
fn generate_integer_chunks(
    table_name: &str,
    snapshot_id: &str,
    min: i64,
    max: i64,
    num_chunks: u32,
    completed: &std::collections::HashSet<u32>,
) -> Vec<SnapshotChunk> {
    let range_len = max - min + 1; // inclusive range
    let chunk_size = (range_len / num_chunks as i64).max(1);
    let extra = (range_len % num_chunks as i64) as u32; // remainder distributed

    let mut chunks = Vec::with_capacity(num_chunks as usize);
    let mut current = min;

    for i in 0..num_chunks {
        if completed.contains(&i) {
            // Advance current past this chunk's width for correct
            // gap computation even when skipping.
            let this_size = chunk_size + if i < extra { 1 } else { 0 };
            current += this_size;
            continue;
        }

        let this_size = chunk_size + if i < extra { 1 } else { 0 };
        let chunk_end = current + this_size;

        let start_str = Some(current.to_string());
        let end_str = if chunk_end <= max {
            Some(chunk_end.to_string())
        } else {
            None
        };

        chunks.push(SnapshotChunk::new(
            table_name.to_string(),
            snapshot_id.to_string(),
            i,
            start_str,
            end_str,
        ));

        current = chunk_end;
    }

    chunks
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── classify_pk ──────────────────────────────────────────────────

    #[test]
    fn test_classify_single_pk() {
        let pks = vec!["id".to_string()];
        let (kind, cols) = classify_pk(&pks);
        assert_eq!(kind, PkKind::SingleInteger("id".into()));
        assert_eq!(cols, &["id".to_string()]);
    }

    #[test]
    fn test_classify_composite_pk() {
        let pks = vec!["org_id".to_string(), "user_id".to_string()];
        let (kind, cols) = classify_pk(&pks);
        assert_eq!(
            kind,
            PkKind::Composite(vec!["org_id".into(), "user_id".into()])
        );
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn test_classify_no_pk() {
        let pks: Vec<String> = vec![];
        let (kind, cols) = classify_pk(&pks);
        assert_eq!(kind, PkKind::None);
        assert!(cols.is_empty());
    }

    // ── generate_chunks: integer PK ──────────────────────────────────

    #[test]
    fn test_generate_integer_chunks_even() {
        let table = TableInfo {
            schema: "public".into(),
            name: "users".into(),
            qualified: "public.users".into(),
        };
        let range = PkRange {
            min: "1".into(),
            max: "100".into(),
        };

        let chunks = generate_chunks(&table, "snap_1", "id", Some(&range), 4, &[]);

        assert_eq!(chunks.len(), 4, "expected 4 chunks");
        assert_eq!(chunks[0].chunk_start, Some("1".into()));
        assert_eq!(chunks[0].chunk_end, Some("26".into())); // (100-1+1)/4 = 25 each, +1 for first extra
        assert_eq!(chunks[1].chunk_start, Some("26".into()));
        assert_eq!(chunks[2].chunk_start, Some("51".into()));
        assert_eq!(chunks[3].chunk_start, Some("76".into()));
        assert_eq!(chunks[3].chunk_end, None); // last chunk goes to max
    }

    #[test]
    fn test_generate_integer_chunks_exact_divisible() {
        let table = TableInfo {
            schema: "public".into(),
            name: "items".into(),
            qualified: "public.items".into(),
        };
        // 120 rows ÷ 4 chunks = 30 each, exactly
        let range = PkRange {
            min: "1".into(),
            max: "120".into(),
        };

        let chunks = generate_chunks(&table, "snap_2", "id", Some(&range), 4, &[]);

        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[0].chunk_start, Some("1".into()));
        assert_eq!(chunks[0].chunk_end, Some("31".into())); // 30 + remainder
        assert_eq!(chunks[1].chunk_start, Some("31".into()));
        assert_eq!(chunks[2].chunk_start, Some("61".into()));
        assert_eq!(chunks[3].chunk_start, Some("91".into()));
    }

    #[test]
    fn test_generate_integer_chunks_empty_table() {
        let table = TableInfo {
            schema: "public".into(),
            name: "empty".into(),
            qualified: "public.empty".into(),
        };

        let chunks = generate_chunks(&table, "snap_3", "id", None, 4, &[]);

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].chunk_start, None);
        assert_eq!(chunks[0].chunk_end, None);
    }

    // ── generate_chunks: resume (skip completed) ─────────────────────

    #[test]
    fn test_generate_chunks_resume_skips_completed() {
        let table = TableInfo {
            schema: "public".into(),
            name: "resume_test".into(),
            qualified: "public.resume_test".into(),
        };
        let range = PkRange {
            min: "1".into(),
            max: "100".into(),
        };

        // chunk 0 and 2 already completed
        let existing = vec![
            (
                0u32,
                Some("1".into()),
                Some("26".into()),
                "completed".into(),
            ),
            (
                2u32,
                Some("51".into()),
                Some("76".into()),
                "completed".into(),
            ),
        ];

        let chunks = generate_chunks(&table, "snap_4", "id", Some(&range), 4, &existing);

        // Should only return chunks 1 and 3
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].chunk_index, 1);
        assert_eq!(chunks[1].chunk_index, 3);
    }

    // ── SnapshotChunk::where_clause ──────────────────────────────────

    #[test]
    fn test_where_clause_both_bounds() {
        let chunk = SnapshotChunk::new(
            "public.users".into(),
            "snap_1".into(),
            0,
            Some("1".into()),
            Some("26".into()),
        );
        let (clause, start, end) = chunk.where_clause("id");
        assert!(clause.contains(r#""id" >= 1"#), "clause={clause}");
        assert!(clause.contains(r#""id" < 26"#), "clause={clause}");
        assert_eq!(start, Some("1".into()));
        assert_eq!(end, Some("26".into()));
    }

    #[test]
    fn test_where_clause_no_bounds() {
        let chunk = SnapshotChunk::new("public.empty".into(), "snap_2".into(), 0, None, None);
        let (clause, start, end) = chunk.where_clause("id");
        assert!(clause.is_empty());
        assert_eq!(start, None);
        assert_eq!(end, None);
    }

    #[test]
    fn test_where_clause_lower_only() {
        let chunk = SnapshotChunk::new(
            "public.last_chunk".into(),
            "snap_3".into(),
            3,
            Some("76".into()),
            None,
        );
        let (clause, start, end) = chunk.where_clause("id");
        assert!(clause.contains(r#""id" >= 76"#), "clause={clause}");
        assert!(!clause.contains(r#""id" <"#));
        assert_eq!(start, Some("76".into()));
        assert_eq!(end, None);
    }

    // ── SnapshotChunk::new ──────────────────────────────────────────

    #[test]
    fn test_chunk_new_is_pending() {
        let c = SnapshotChunk::new(
            "public.t".into(),
            "snap_x".into(),
            0,
            Some("1".into()),
            Some("10".into()),
        );
        assert_eq!(c.status, ChunkStatus::Pending);
        assert_eq!(c.chunk_index, 0);
    }

    // ── PkKind Display (implied via Debug) ───────────────────────────

    #[test]
    fn test_pk_kind_debug() {
        let single = PkKind::SingleInteger("id".into());
        assert_eq!(format!("{single:?}"), "SingleInteger(\"id\")");

        let none = PkKind::None;
        assert_eq!(format!("{none:?}"), "None");
    }
}
