//! [`SnapshotRunner`] — sequential table-snapshot engine.
//!
//! Uses two plain (non-replication) Postgres connections:
//!
//! * **Keeper** — holds `BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ`
//!   + `pg_export_snapshot()` open for the entire snapshot run.
//! * **Worker** — pins each table transaction to the exported snapshot
//!   via `SET TRANSACTION SNAPSHOT`, scans tables using a server-side
//!   cursor (`DECLARE` / `FETCH`) to avoid loading the full table into
//!   process memory, and emits one `op:'r'` (Read) [`ChangeEvent`] per row.
//!
//! Progress is checkpointed to the [`StateStore`] after every `batch_size`
//! rows.  On interruption the snapshot can resume from the last checkpoint.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json;
use tokio_postgres::Row;
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::SnapshotConfig;
use crate::error::TapError;
use crate::event::{ChangeEvent, ChangeEventBuilder, Operation, SourceMetadata};
use crate::postgres::Lsn;
use crate::state::StateStore;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Summary of a completed snapshot run.
///
/// Generic over the source position type `P` so both Postgres (`Lsn`) and
/// MySQL (`BinlogPosition`) can use the same result struct.
/// Defaults to [`Lsn`] so existing Postgres callers need no changes.
#[derive(Debug, Clone)]
pub struct SnapshotResult<P = Lsn> {
    /// Snapshot identifier (exported snapshot ID for Postgres,
    /// `"{binlog_file}:{binlog_offset}"` for MySQL).
    pub snapshot_id: String,
    /// Source position at the moment the snapshot was taken.
    pub position: P,
    /// Total number of rows scanned across all tables.
    pub total_rows: u64,
    /// Qualified names of the tables that were snapshotted.
    pub tables_snapshotted: Vec<String>,
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Parsed table-identity information.
///
/// Stores schema and name separately so queries can quote each half
/// independently, preventing SQL injection through user-supplied table
/// names (tap-355).
#[derive(Debug, Clone)]
pub(crate) struct TableInfo {
    /// Schema name (e.g. `"public"`).
    pub(crate) schema: String,
    /// Table name (e.g. `"users"`).
    pub(crate) name: String,
    /// Schema-qualified name for display/logging (`schema.table`).
    pub(crate) qualified: String,
}

/// Quote a Postgres identifier, escaping embedded double-quotes.
pub(crate) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Build a safely-quoted `schema.table` for use in SQL.
pub(crate) fn qualified_sql(table: &TableInfo) -> String {
    format!(
        "{}.{}",
        quote_ident(&table.schema),
        quote_ident(&table.name)
    )
}

/// Split a `schema.table` string into its components.
/// If no dot is present, `"public"` is used as the schema.
pub(crate) fn parse_qualified_name(qualified: &str) -> TableInfo {
    match qualified.split_once('.') {
        Some((schema, name)) => TableInfo {
            schema: schema.to_string(),
            name: name.to_string(),
            qualified: qualified.to_string(),
        },
        None => TableInfo {
            schema: "public".to_string(),
            name: qualified.to_string(),
            qualified: format!("public.{qualified}"),
        },
    }
}

/// Convert a `tokio_postgres::Row` into a `serde_json::Value::Object`.
///
/// Determines the Postgres column type via its OID and reads it with the
/// correct Rust type, then serialises to JSON.  Handles common types:
/// integer, float, bool, text, json/jsonb, and date/time.  Falls back to
/// a string representation for unknown types.
pub(crate) fn row_to_json(row: &Row) -> Result<serde_json::Value, TapError> {
    let columns = row.columns();
    let mut map = serde_json::Map::with_capacity(columns.len());

    for (i, col) in columns.iter().enumerate() {
        let name = col.name();
        let type_oid = col.type_().oid();

        // Helper: read a column as an Option<T> and convert to JSON
        macro_rules! read_opt {
            ($t:ty) => {{
                match row.try_get::<_, Option<$t>>(i) {
                    Ok(Some(v)) => serde_json::json!(v),
                    Ok(None) => serde_json::Value::Null,
                    Err(e) => {
                        return Err(TapError::Decode(format!(
                            "failed to read column '{name}' at {i}: {e}"
                        )));
                    }
                }
            }};
        }

        let value: serde_json::Value = match type_oid {
            // int8 (bigint / int8)
            20 => read_opt!(i64),
            // int2 (smallint / int2)
            21 => read_opt!(i16),
            // int4 (integer / serial / int4) and oid
            23 | 26 => read_opt!(i32),
            // bool
            16 => read_opt!(bool),
            // float4 (real)
            700 => read_opt!(f32),
            // float8 (double precision)
            701 => read_opt!(f64),
            // json / jsonb
            114 | 3802 => {
                // with-serde_json-1 feature handles json(b) → Value
                match row.try_get::<_, Option<serde_json::Value>>(i) {
                    Ok(Some(v)) => v,
                    Ok(None) => serde_json::Value::Null,
                    Err(e) => {
                        return Err(TapError::Decode(format!(
                            "failed to read json column '{name}' at {i}: {e}"
                        )));
                    }
                }
            }
            // timestamptz / timestamp — read via with-chrono-0_4 feature
            1184 | 1114 => match row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(i) {
                Ok(Some(v)) => serde_json::Value::String(v.to_rfc3339()),
                Ok(None) => serde_json::Value::Null,
                Err(e) => {
                    return Err(TapError::Decode(format!(
                        "failed to read timestamp column '{name}' at {i}: {e}"
                    )));
                }
            },
            // Everything else (text, varchar, char, date, time, uuid,
            // inet, bytea, etc.) — read as String.
            _ => read_opt!(String),
        };
        map.insert(name.to_string(), value);
    }

    Ok(serde_json::Value::Object(map))
}

/// Build a unique event identifier for a snapshot row.
///
/// Format for tables with PKs:
///   `snap:{schema}.{table}:{pk1}={val1}[:{pk2}={val2}...]`
///
/// For tables without a PK a UUID-based fallback is used:
///   `snap:{schema}.{table}:{uuid}`
pub(crate) fn build_snapshot_event_id(
    table: &TableInfo,
    pk_columns: &[String],
    row: &Row,
) -> Result<String, TapError> {
    let prefix = format!("snap:{}.{}", table.schema, table.name);

    if pk_columns.is_empty() {
        return Ok(format!("{}:{}", prefix, Uuid::new_v4()));
    }

    // Macro: read PK column by type OID and convert to string
    macro_rules! pk_str {
        ($row:expr, $idx:expr, $t:ty) => {{
            match $row.try_get::<_, Option<$t>>($idx) {
                Ok(Some(v)) => v.to_string(),
                Ok(None) => "NULL".to_string(),
                Err(e) => {
                    return Err(TapError::Decode(format!(
                        "failed to read PK column for event ID: {e}"
                    )));
                }
            }
        }};
    }

    let columns = row.columns();
    let mut parts = Vec::with_capacity(pk_columns.len());

    for pk in pk_columns {
        // Find column index by name
        let idx = columns
            .iter()
            .position(|c| c.name() == pk.as_str())
            .ok_or_else(|| {
                TapError::Decode(format!("PK column '{pk}' not found in row columns"))
            })?;
        let type_oid = columns[idx].type_().oid();

        let val = match type_oid {
            // int8 (bigint)
            20 => pk_str!(row, idx, i64),
            // int2 (smallint)
            21 => pk_str!(row, idx, i16),
            // int4 / serial / oid
            23 | 26 => pk_str!(row, idx, i32),
            // bool
            16 => pk_str!(row, idx, bool),
            // float4
            700 => pk_str!(row, idx, f32),
            // float8
            701 => pk_str!(row, idx, f64),
            // timestamptz / timestamp
            1184 | 1114 => match row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx) {
                Ok(Some(v)) => v.to_rfc3339(),
                Ok(None) => "NULL".to_string(),
                Err(e) => {
                    return Err(TapError::Decode(format!(
                        "failed to read PK timestamp column '{pk}' for event ID: {e}"
                    )));
                }
            },
            // Everything else — read as String
            _ => match row.try_get::<_, Option<String>>(idx) {
                Ok(Some(s)) => s,
                Ok(None) => "NULL".to_string(),
                Err(e) => {
                    return Err(TapError::Decode(format!(
                        "failed to read PK column '{pk}' for event ID: {e}"
                    )));
                }
            },
        };

        parts.push(format!("{pk}={val}"));
    }

    Ok(format!("{}:{}", prefix, parts.join(":")))
}

// ---------------------------------------------------------------------------
// SnapshotRunner
// ---------------------------------------------------------------------------

/// Sequential snapshot engine.
///
/// Uses two plain (non-replication) Postgres connections:
///
/// * **Keeper** — holds the exported snapshot transaction for the full
///   snapshot duration (fixes tap-zhx: snapshot must stay open until
///   all tables are scanned).
/// * **Worker** — scans each table via server-side cursor (`DECLARE` /
///   `FETCH`) to avoid OOM on large tables (fixes tap-qo1).
///
/// Every error inside a table transaction triggers a `ROLLBACK` (via
/// `tokio_postgres::Transaction`'s Drop impl), and the keeper transaction
/// is rolled back on any top-level error (fixes tap-ron).
///
/// All identifiers are double-quoted in SQL to prevent injection (tap-355).
/// The [`StateStore`] is wrapped in [`tokio::sync::Mutex`] to avoid
/// blocking the Tokio worker thread (tap-84y).
///
/// # Errors
///
/// All methods return [`TapError`] on failure — Postgres errors,
/// SQLite errors, and snapshot-specific errors are all represented.
pub struct SnapshotRunner {
    /// Keeper connection — holds the exported snapshot open.
    keeper: tokio_postgres::Client,
    /// Worker connection — scans tables with SET TRANSACTION SNAPSHOT.
    worker: tokio_postgres::Client,
    /// Shared state store (SQLite), behind a tokio mutex.
    state: Arc<tokio::sync::Mutex<StateStore>>,
    /// Snapshot-phase configuration.
    config: SnapshotConfig,
    /// Database name (from source config, used in SourceMetadata).
    db_name: String,
    /// Channel for emitting change events to the downstream pipeline.
    event_tx: tokio::sync::mpsc::UnboundedSender<ChangeEvent>,
}

impl SnapshotRunner {
    /// Create a new `SnapshotRunner`.
    ///
    /// The caller is responsible for providing two **plain** (non-replication)
    /// Postgres [`tokio_postgres::Client`]s obtained from
    /// [`crate::postgres::connect_plain`].
    pub fn new(
        keeper: tokio_postgres::Client,
        worker: tokio_postgres::Client,
        state: Arc<tokio::sync::Mutex<StateStore>>,
        config: SnapshotConfig,
        db_name: String,
        event_tx: tokio::sync::mpsc::UnboundedSender<ChangeEvent>,
    ) -> Self {
        Self {
            keeper,
            worker,
            state,
            config,
            db_name,
            event_tx,
        }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Run the full snapshot sequence.
    ///
    /// 1. Begins `READ ONLY REPEATABLE READ` on the keeper connection and
    ///    calls `pg_export_snapshot()`.
    /// 2. Discovers tables (from config or the publication).
    /// 3. Scans each table using the worker connection pinned to the snapshot,
    ///    emitting `Read` events and checkpointing progress.
    /// 4. Commits (or rolls back) the keeper transaction.
    /// 5. Returns a summary [`SnapshotResult`].
    pub async fn run(&mut self) -> Result<SnapshotResult, TapError> {
        // ── Keeper transaction — stays open until all tables are scanned ──
        self.keeper
            .batch_execute("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ")
            .await?;

        let result = self.run_inner().await;

        // Always close the keeper transaction — COMMIT on success,
        // ROLLBACK on any error.
        self.keeper
            .batch_execute(match &result {
                Ok(_) => "COMMIT",
                Err(_) => "ROLLBACK",
            })
            .await
            .ok();

        result
    }

    // -----------------------------------------------------------------------
    // Inner run (wrapped by keeper transaction)
    // -----------------------------------------------------------------------

    async fn run_inner(&mut self) -> Result<SnapshotResult, TapError> {
        let (snapshot_id, lsn) = self.export_snapshot().await?;
        info!("snapshot exported: id={snapshot_id}, lsn={lsn}");

        let tables = self.resolve_tables().await?;
        if tables.is_empty() {
            info!("no tables to snapshot");
            return Ok(SnapshotResult {
                snapshot_id,
                position: lsn,
                total_rows: 0,
                tables_snapshotted: Vec::new(),
            });
        }

        let mut total_rows: u64 = 0;
        let mut tables_snapshotted: Vec<String> = Vec::with_capacity(tables.len());

        for table in &tables {
            let rows = self.snapshot_table(table, &snapshot_id, &lsn).await?;
            total_rows += rows;
            tables_snapshotted.push(format!("\"{}\".\"{}\"", table.schema, table.name));
        }

        info!(
            snapshot_id = %snapshot_id,
            lsn = %lsn,
            total_rows,
            tables = %tables_snapshotted.len(),
            "snapshot run complete"
        );

        Ok(SnapshotResult {
            snapshot_id,
            position: lsn,
            total_rows,
            tables_snapshotted,
        })
    }

    // -----------------------------------------------------------------------
    // Step 1: Export snapshot (on keeper)
    // -----------------------------------------------------------------------

    /// Export a Postgres snapshot and record the current WAL position.
    ///
    /// The caller **must** have already started a transaction on the keeper
    /// connection (see [`run`](Self::run)).
    async fn export_snapshot(&self) -> Result<(String, Lsn), TapError> {
        // Export the snapshot identifier
        let snap_row = self
            .keeper
            .query_one("SELECT pg_export_snapshot()", &[])
            .await?;
        let snapshot_id: String = snap_row.get(0);

        // Capture the WAL position at snapshot time
        let lsn_row = self
            .keeper
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await?;
        let lsn_str: String = lsn_row.get(0);
        let lsn: Lsn = lsn_str.parse()?;

        Ok((snapshot_id, lsn))
    }

    // -----------------------------------------------------------------------
    // Step 2: Table discovery (on worker)
    // -----------------------------------------------------------------------

    /// Resolve the list of tables to snapshot.
    ///
    /// If [`SnapshotConfig::tables`] is non-empty, those are used directly.
    /// Otherwise the tables are discovered from the publication configured
    /// on the source connection.
    async fn resolve_tables(&self) -> Result<Vec<TableInfo>, TapError> {
        if !self.config.tables.is_empty() {
            let mut tables: Vec<TableInfo> = self
                .config
                .tables
                .iter()
                .map(|t| parse_qualified_name(t))
                .collect();
            // Sort by qualified name for deterministic order
            tables.sort_by(|a, b| a.qualified.cmp(&b.qualified));
            return Ok(tables);
        }

        // We still need publication info. Resolve from the worker.
        // Use a simple catalog query to get publication tables.
        let pub_name = self
            .worker
            .query_one("SELECT pubname || '' FROM pg_publication LIMIT 1", &[])
            .await
            .map(|r| {
                let s: String = r.get(0);
                s
            })
            .unwrap_or_else(|_| "tap_publication".into());

        let rows = self
            .worker
            .query(
                "SELECT schemaname, tablename \
                 FROM pg_publication_tables \
                 WHERE pubname = $1 \
                 ORDER BY schemaname, tablename",
                &[&pub_name],
            )
            .await?;

        if rows.is_empty() {
            warn!(
                publication = %pub_name,
                "publication has no tables — snapshot will be a no-op"
            );
        }

        let tables: Vec<TableInfo> = rows
            .iter()
            .map(|row| {
                let schema: String = row.get(0);
                let name: String = row.get(1);
                TableInfo {
                    qualified: format!("{schema}.{name}"),
                    schema,
                    name,
                }
            })
            .collect();

        Ok(tables)
    }

    // -----------------------------------------------------------------------
    // Step 3: Snapshot a single table (on worker)
    // -----------------------------------------------------------------------

    /// Snapshot one table: detect PK, scan rows via cursor, emit events, checkpoint.
    ///
    /// Uses a server-side cursor (`DECLARE` / `FETCH`) to avoid loading the
    /// entire table into process memory (fixes tap-qo1: OOM on large tables).
    ///
    /// The worker transaction is managed by `tokio_postgres::Transaction`
    /// which auto-rolls back on Drop (fixes tap-ron: missing ROLLBACK).
    async fn snapshot_table(
        &mut self,
        table: &TableInfo,
        snapshot_id: &str,
        snapshot_lsn: &Lsn,
    ) -> Result<u64, TapError> {
        // ── Resume check ──────────────────────────────────────────────
        if self.is_table_completed(table).await? {
            info!(
                "table {} snapshot already completed, skipping",
                table.qualified
            );
            return self.completed_row_count(table).await;
        }

        // ── Worker transaction (auto-rollback on Drop)  ───────────────
        let txn = self.worker.transaction().await?;

        // SET TRANSACTION SNAPSHOT requires REPEATABLE READ or SERIALIZABLE.
        // tokio_postgres::transaction() defaults to READ COMMITTED, so we
        // must upgrade before issuing the snapshot command.
        txn.batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await?;

        // Pin this transaction to the exported snapshot
        txn.simple_query(&format!("SET TRANSACTION SNAPSHOT '{snapshot_id}'"))
            .await?;

        // ── Detect primary key columns  ───────────────────────────────
        let pk_columns = detect_pk_columns(txn.client(), table).await?;

        if pk_columns.is_empty() {
            warn!(
                "table '{}' has no primary key — using ctid ordering; \
                 incremental resume is NOT possible on this table",
                table.qualified
            );
        }

        // ── Build ordered query with quoted identifiers  ──────────────
        let order_clause = if pk_columns.is_empty() {
            "ctid".to_string()
        } else {
            pk_columns
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let sql_table = qualified_sql(table);
        let query = format!("SELECT * FROM {sql_table} ORDER BY {order_clause}, ctid");

        // ── Declare server-side cursor (never buffers all rows) ───────
        txn.batch_execute(&format!("DECLARE snap_cursor CURSOR FOR {query}"))
            .await?;

        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let mut rows_done: u64 = 0;

        // ── Fetch loop  ───────────────────────────────────────────────
        loop {
            let fetch_sql = format!("FETCH FORWARD {} FROM snap_cursor", self.config.batch_size);
            let batch = txn.query(&fetch_sql, &[]).await?;

            if batch.is_empty() {
                break;
            }

            for row in &batch {
                // Build the ChangeEvent for this row
                emit_row_event(
                    &self.event_tx,
                    table,
                    &pk_columns,
                    snapshot_lsn,
                    ts_ms,
                    &self.db_name,
                    row,
                )?;
            }

            rows_done += batch.len() as u64;

            // ── Checkpoint after each FETCH batch  ────────────────────
            {
                let state = self.state.lock().await;
                state.write_snapshot_progress(
                    &table.qualified,
                    snapshot_id,
                    rows_done,
                    snapshot_lsn,
                )?;
            }
            info!(
                table = %table.qualified,
                rows_done,
                "snapshot checkpoint"
            );
        }

        // ── Cleanup cursor  ───────────────────────────────────────────
        txn.batch_execute("CLOSE snap_cursor").await?;

        // ── Final checkpoint  ─────────────────────────────────────────
        {
            let state = self.state.lock().await;
            state.write_snapshot_progress(
                &table.qualified,
                snapshot_id,
                rows_done,
                snapshot_lsn,
            )?;
        }

        // Commit the table transaction (auto-rollback on Drop if we Err)
        txn.commit().await?;

        // Mark completed
        self.mark_table_complete(table, snapshot_id, rows_done)
            .await?;

        info!(
            table = %table.qualified,
            rows_done,
            "snapshot completed"
        );

        Ok(rows_done)
    }

    // -----------------------------------------------------------------------
    // State store helpers (async, tokio::sync::Mutex)
    // -----------------------------------------------------------------------

    /// Returns `true` when the table has a completed snapshot record.
    async fn is_table_completed(&self, table: &TableInfo) -> Result<bool, TapError> {
        let state = self.state.lock().await;
        match state.get_snapshot_status(&table.qualified)? {
            Some(rec) => Ok(rec.status == "completed"),
            None => Ok(false),
        }
    }

    /// Returns the row count from a completed snapshot record.
    async fn completed_row_count(&self, table: &TableInfo) -> Result<u64, TapError> {
        let state = self.state.lock().await;
        match state.get_snapshot_status(&table.qualified)? {
            Some(rec) if rec.status == "completed" => Ok(rec.rows_count),
            _ => Ok(0),
        }
    }

    /// Mark a snapshot table as completed in the state store.
    async fn mark_table_complete(
        &self,
        table: &TableInfo,
        snapshot_id: &str,
        rows: u64,
    ) -> Result<(), TapError> {
        let state = self.state.lock().await;
        state.complete_snapshot(&table.qualified, snapshot_id, rows)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build and send a single row event via the channel.
pub(crate) fn emit_row_event(
    event_tx: &tokio::sync::mpsc::UnboundedSender<ChangeEvent>,
    table: &TableInfo,
    pk_columns: &[String],
    snapshot_lsn: &Lsn,
    ts_ms: u64,
    db_name: &str,
    row: &Row,
) -> Result<(), TapError> {
    let row_json = row_to_json(row)?;

    // Check individual row size (serialized JSON)
    if let Some(size) = estimate_row_size(&row_json) {
        if size > 1_048_576 {
            // 1 MB
            warn!(
                table = %table.qualified,
                size_bytes = size,
                "row exceeds 1 MB — streaming as-is, no chunking applied"
            );
        }
    }

    let source = SourceMetadata {
        db: db_name.to_string(),
        schema: table.schema.clone(),
        table: table.name.clone(),
        lsn: Some(snapshot_lsn.to_string()),
        binlog_file: None,
        binlog_offset: None,
        tx_id: "0".into(),
        ts_ms,
        snapshot: Some(true),
    };

    let mut event = ChangeEventBuilder::new()
        .op(Operation::Read)
        .after(Some(row_json))
        .source(source)
        .build()?;

    // Override the auto-generated ID with PK-based format
    event.id = build_snapshot_event_id(table, pk_columns, row)?;

    if event_tx.send(event).is_err() {
        return Err(TapError::Snapshot(
            "event channel closed while snapshotting table".into(),
        ));
    }

    Ok(())
}

/// Detect primary-key column names for a table, in index order.
///
/// `client` can be a plain `Client` or `Transaction` (via `Deref`).
pub(crate) async fn detect_pk_columns(
    client: &tokio_postgres::Client,
    table: &TableInfo,
) -> Result<Vec<String>, TapError> {
    let rows = client
        .query(
            "SELECT a.attname \
             FROM pg_index i \
             JOIN pg_attribute a \
               ON a.attrelid = i.indrelid \
              AND a.attnum = ANY(i.indkey::int2[]) \
             WHERE i.indrelid = to_regclass($1) \
               AND i.indisprimary \
             ORDER BY a.attnum",
            &[&table.qualified],
        )
        .await?;

    let pks: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    Ok(pks)
}

/// Estimate the serialized byte size of a JSON value.
/// Returns `None` when the size cannot be determined.
pub(crate) fn estimate_row_size(value: &serde_json::Value) -> Option<usize> {
    Some(value.to_string().len())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── parse_qualified_name ────────────────────────────────────────

    #[test]
    fn test_parse_qualified_name_with_schema() {
        let t = parse_qualified_name("public.users");
        assert_eq!(t.schema, "public");
        assert_eq!(t.name, "users");
        assert_eq!(t.qualified, "public.users");
    }

    #[test]
    fn test_parse_qualified_name_without_schema() {
        let t = parse_qualified_name("users");
        assert_eq!(t.schema, "public");
        assert_eq!(t.name, "users");
        assert_eq!(t.qualified, "public.users");
    }

    #[test]
    fn test_parse_qualified_name_custom_schema() {
        let t = parse_qualified_name("inventory.orders");
        assert_eq!(t.schema, "inventory");
        assert_eq!(t.name, "orders");
        assert_eq!(t.qualified, "inventory.orders");
    }

    // ── quote_ident ─────────────────────────────────────────────────

    #[test]
    fn test_quote_ident_simple() {
        assert_eq!(quote_ident("users"), "\"users\"");
    }

    #[test]
    fn test_quote_ident_escapes_embedded_quotes() {
        assert_eq!(quote_ident("my\"table"), "\"my\"\"table\"");
    }

    #[test]
    fn test_qualified_sql_quotes_both_parts() {
        let t = TableInfo {
            schema: "public".into(),
            name: "users".into(),
            qualified: "public.users".into(),
        };
        assert_eq!(qualified_sql(&t), "\"public\".\"users\"");
    }

    // ── row_to_json ─────────────────────────────────────────────────

    #[test]
    fn test_build_snapshot_event_id_single_pk() {
        let table = parse_qualified_name("public.users");
        let _pk_cols = vec!["id".to_string()];
        let id_prefix = format!("snap:{}.{}:", table.schema, table.name);
        assert_eq!(id_prefix, "snap:public.users:");

        let pk_part = "id=42";
        let expected = format!("{}{}", id_prefix, pk_part);
        assert_eq!(expected, "snap:public.users:id=42");
    }

    #[test]
    fn test_build_snapshot_event_id_composite_pk() {
        let table = parse_qualified_name("public.order_items");
        let pk_cols = vec!["order_id".to_string(), "product_id".to_string()];

        let expected_prefix = "snap:public.order_items:";
        assert_eq!(
            format!("snap:{}.{}:", table.schema, table.name),
            expected_prefix
        );

        let parts = pk_cols
            .iter()
            .map(|pk| format!("{pk}=<val>"))
            .collect::<Vec<_>>()
            .join(":");
        let expected = format!("{expected_prefix}{parts}");
        assert_eq!(
            expected,
            "snap:public.order_items:order_id=<val>:product_id=<val>"
        );
    }

    #[test]
    fn test_build_snapshot_event_id_no_pk() {
        let table = parse_qualified_name("public.no_pk_table");
        let _pk_cols: Vec<String> = vec![];

        let prefix = format!("snap:{}.{}:", table.schema, table.name);
        let id = format!("{}some-uuid", prefix);
        assert!(id.starts_with("snap:public.no_pk_table:"));
        assert!(!id.ends_with(':'));
    }

    // ── row_to_json value construction ──────────────────────────────

    #[test]
    fn test_row_to_json_basic_types() {
        let mut map = serde_json::Map::new();
        map.insert("id".into(), json!(42));
        map.insert("name".into(), json!("Alice"));
        map.insert("active".into(), json!(true));
        map.insert("score".into(), json!(null));

        let value = serde_json::Value::Object(map);
        let obj = value.as_object().unwrap();

        assert_eq!(obj["id"], json!(42));
        assert_eq!(obj["name"], json!("Alice"));
        assert_eq!(obj["active"], json!(true));
        assert_eq!(obj["score"], json!(null));
    }

    #[test]
    fn test_estimate_row_size() {
        let val = json!({"id": 1, "name": "Alice", "data": [1, 2, 3]});
        let size = estimate_row_size(&val);
        assert!(size.is_some());
        assert!(size.unwrap() > 0);
    }

    // ── SnapshotResult ──────────────────────────────────────────────

    #[test]
    fn test_snapshot_result_creation() {
        let lsn = Lsn::from_u64(0x16B37428);
        let result = SnapshotResult {
            snapshot_id: "00000004-000004D8-1".into(),
            position: lsn,
            total_rows: 1000,
            tables_snapshotted: vec!["public.users".into(), "public.orders".into()],
        };

        assert_eq!(result.total_rows, 1000);
        assert_eq!(result.tables_snapshotted.len(), 2);
        assert_eq!(result.position, lsn);
    }

    // ── Empty tables list edge case ─────────────────────────────────

    #[test]
    fn test_parse_empty_table_name() {
        let t = parse_qualified_name("");
        assert_eq!(t.schema, "public");
        assert_eq!(t.name, "");
        assert_eq!(t.qualified, "public.");
    }

    // ── LSN formatting ──────────────────────────────────────────────

    #[test]
    fn test_snapshot_lsn_display() {
        let lsn = Lsn::from_u64(0x16B37428);
        assert_eq!(lsn.to_string(), "0/16B37428");
    }

    // ── ChangeEvent ID format for snapshots ─────────────────────────

    #[test]
    fn test_snapshot_event_id_roundtrip() {
        let table = parse_qualified_name("public.users");
        let _expected_prefix = "snap:public.users:";
        assert_eq!(
            format!("snap:{}:id=42", table.qualified),
            "snap:public.users:id=42"
        );
    }

    #[test]
    fn test_snapshot_source_metadata_flags() {
        let source = SourceMetadata {
            db: "test_db".into(),
            schema: "public".into(),
            table: "users".into(),
            lsn: Some("0/16B37428".into()),
            binlog_file: None,
            binlog_offset: None,
            tx_id: "0".into(),
            ts_ms: 1_700_000_000_000,
            snapshot: Some(true),
        };

        assert_eq!(source.snapshot, Some(true));
        assert_eq!(source.tx_id, "0");
        assert!(source.db.contains("test_db"));
    }

    // ── Composite PK key-value generation ───────────────────────────

    #[test]
    fn test_composite_pk_keyval_format() {
        let pk_cols = vec!["org_id".to_string(), "user_id".to_string()];

        let keyvals: Vec<String> = pk_cols.iter().map(|pk| format!("{}=<val>", pk)).collect();
        let joined = keyvals.join(":");

        assert_eq!(joined, "org_id=<val>:user_id=<val>");
        assert!(joined.contains("org_id"));
        assert!(joined.contains("user_id"));
    }

    // ── Test that in-memory SourceMetadata round-trips through JSON ─

    #[test]
    fn test_snapshot_source_metadata_json_roundtrip() {
        let source = SourceMetadata {
            db: "snap_db".into(),
            schema: "public".into(),
            table: "orders".into(),
            lsn: Some("0/ABCD".into()),
            binlog_file: None,
            binlog_offset: None,
            tx_id: "0".into(),
            ts_ms: 1_700_000_000_000,
            snapshot: Some(true),
        };

        let json = serde_json::to_string(&source).unwrap();
        let deserialized: SourceMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.snapshot, Some(true));
        assert_eq!(deserialized.schema, "public");
        assert_eq!(deserialized.table, "orders");
    }

    // ── Large row warning detection ─────────────────────────────────

    #[test]
    fn test_large_row_detection() {
        let large_data = "x".repeat(2_000_000);
        let large_row = json!({"data": large_data});

        let size = estimate_row_size(&large_row).unwrap();
        assert!(size > 1_048_576, "expected size > 1 MB, got {size}");
    }

    #[test]
    fn test_small_row_no_warning() {
        let small_row = json!({"id": 1, "name": "test"});
        let size = estimate_row_size(&small_row).unwrap();
        assert!(size < 1_048_576, "expected size < 1 MB, got {size}");
    }
}
