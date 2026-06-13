//! MySQL parallel snapshot engine.
//!
//! Implements the FLUSH TABLES WITH READ LOCK + binlog position protocol
//! for consistent parallel snapshots of MySQL tables.
//!
//! ## Protocol
//!
//! 1. **Keeper connection**: acquires FTWRL, records binlog position via
//!    `SHOW MASTER STATUS`, then releases the lock.
//! 2. **Metadata connection**: resolves tables, detects PK columns via
//!    `information_schema`, generates PK-range chunks using the shared
//!    [`crate::snapshot::chunker`] (same integer-range splitting as Postgres).
//! 3. **Worker connections**: each worker opens its own connection, starts
//!    a transaction with `START TRANSACTION WITH CONSISTENT SNAPSHOT`,
//!    then scans assigned PK-range chunks emitting `op='r'` events.
//!
//! Unlike the Postgres parallel engine, MySQL has no exported-snapshot
//! mechanism (`pg_export_snapshot`). Each worker independently starts a
//! consistent snapshot. The binlog position captured by the keeper is
//! used as the snapshot identifier.
//!
//! Lock is held only during binlog position acquisition (target < 5 s).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use mysql_async::prelude::*;
use mysql_async::{Conn, Pool, Row as MyRow, Value as MyValue};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tracing::{error, info, warn};

use crate::config::{MySqlSourceConfig, SnapshotConfig};
use crate::error::TapError;
use crate::event::{
    builder::ChangeEventBuilder, ChangeEvent, Lsn, Operation, SourceMetadata,
};
use crate::snapshot::chunker::{generate_chunks, PkRange, SnapshotChunk};
use crate::snapshot::runner::TableInfo;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Run a parallel snapshot against MySQL tables.
///
/// # Arguments
/// * `source_config` — MySQL source connection parameters.
/// * `snap_config` — Snapshot parameters (batch size, number of workers, tables).
/// * `event_tx` — Channel to emit snapshot `ChangeEvent`s.
///
/// # Returns
/// A `(snapshot_id, total_rows)` tuple.
/// The `snapshot_id` is `"{binlog_file}:{binlog_offset}"`.
pub async fn run_mysql_parallel_snapshot(
    source_config: &MySqlSourceConfig,
    snap_config: &SnapshotConfig,
    event_tx: &UnboundedSender<ChangeEvent>,
) -> Result<(String, u64), TapError> {
    // Build connection pool from source config.
    let pool_opts = source_config.opts();
    let pool = Pool::new(pool_opts);

    // ── Step 1: Keeper — acquire binlog position ──────────────────────
    let mut keeper = pool
        .get_conn()
        .await
        .map_err(|e| TapError::Snapshot(format!("keeper connect failed: {e}")))?;

    let (binlog_file, binlog_offset) = acquire_binlog_position(&mut keeper).await?;
    let snapshot_id = format!("{binlog_file}:{binlog_offset}");
    let lsn = Lsn(binlog_offset.to_string());
    drop(keeper);

    info!(
        binlog_file,
        binlog_offset,
        num_workers = snap_config.num_workers,
        "acquired MySQL binlog position",
    );

    // ── Step 2: Resolve tables ───────────────────────────────────────
    let tables = resolve_tables(source_config, snap_config);
    if tables.is_empty() {
        return Err(TapError::Snapshot(
            "no tables to snapshot: check snapshot.tables config".into(),
        ));
    }

    // ── Step 3: Metadata connection — detect PKs, generate chunks ────
    let mut meta_conn = pool
        .get_conn()
        .await
        .map_err(|e| TapError::Snapshot(format!("metadata connect failed: {e}")))?;

    struct TableWork {
        table: TableInfo,
        pks: Vec<String>,
        chunks: Vec<SnapshotChunk>,
    }

    let mut table_work_list: Vec<TableWork> = Vec::new();

    for table in &tables {
        let pks = mysql_detect_pk_columns(&mut meta_conn, table).await?;
        if pks.is_empty() {
            warn!(table = table.qualified, "no PK — skipping");
            continue;
        }
        info!(table = table.qualified, pks = ?pks, "detected PK columns");

        let num_chunks = snap_config.num_workers;
        let pk_col = &pks[0];

        // Query MIN/MAX of the PK.
        let range_sql = format!(
            "SELECT MIN(`{pk_col}`) AS min_pk, MAX(`{pk_col}`) AS max_pk FROM {}",
            table.qualified,
        );

        let mut result = meta_conn.query_iter(range_sql.as_str()).await.map_err(|e| {
            TapError::Snapshot(format!("PK range query failed for {}: {e}", table.qualified))
        })?;

        let rows: Vec<MyRow> = result.collect().await.map_err(|e| {
            TapError::Snapshot(format!("PK range collect failed for {}: {e}", table.qualified))
        })?;

        let pk_range = rows.into_iter().next().and_then(|r| {
            let min_val: Option<String> = mysql_get_string(&r, "min_pk");
            let max_val: Option<String> = mysql_get_string(&r, "max_pk");
            match (min_val, max_val) {
                (Some(min), Some(max)) if !min.is_empty() && min != max => {
                    Some(PkRange { min, max })
                }
                _ => None,
            }
        });

        let chunks = generate_chunks(table, &snapshot_id, pk_col, pk_range.as_ref(), num_chunks, &[]);

        table_work_list.push(TableWork {
            table: table.clone(),
            pks,
            chunks,
        });
    }
    drop(meta_conn);

    if table_work_list.is_empty() {
        return Err(TapError::Snapshot("no snapshotable tables found".into()));
    }

    let total_table_count = table_work_list.len();

    // ── Step 4: Distribute chunks to workers round-robin ──────────────
    let worker_count = snap_config.num_workers as usize;
    let total_chunks: usize = table_work_list.iter().map(|t| t.chunks.len()).sum();
    info!(
        tables = total_table_count,
        chunks = total_chunks,
        workers = worker_count,
        "generated snapshot chunks",
    );

    struct WorkerWork {
        table: TableInfo,
        pks: Vec<String>,
        rx: UnboundedReceiver<SnapshotChunk>,
    }

    let mut worker_channels: Vec<UnboundedSender<SnapshotChunk>> = Vec::new();
    let mut worker_work: Vec<WorkerWork> = Vec::new();

    for _ in 0..worker_count {
        let (tx, rx) = mpsc::unbounded_channel();
        worker_channels.push(tx);
        worker_work.push(WorkerWork {
            table: TableInfo {
                schema: String::new(),
                name: String::new(),
                qualified: String::new(),
            },
            pks: Vec::new(),
            rx,
        });
    }

    let mut chunk_idx = 0usize;
    for tw in &table_work_list {
        for chunk in &tw.chunks {
            let wi = chunk_idx % worker_count;
            if worker_channels[wi].send(chunk.clone()).is_err() {
                error!(worker = wi, "failed to send chunk");
            }
            worker_work[wi].table = tw.table.clone();
            worker_work[wi].pks = tw.pks.clone();
            chunk_idx += 1;
        }
    }
    drop(worker_channels);

    // ── Step 5: Spawn workers ─────────────────────────────────────────
    let event_tx = Arc::new(event_tx.clone());
    let pool = Arc::new(pool);
    let mut handles = Vec::new();
    let batch_size = snap_config.batch_size as u32;

    for (wi, assignment) in worker_work.into_iter().enumerate() {
        if assignment.table.name.is_empty() {
            continue;
        }
        let pool = pool.clone();
        let event_tx = event_tx.clone();
        let lsn = lsn.clone();
        let table = assignment.table;
        let pks = assignment.pks;

        let handle: tokio::task::JoinHandle<Result<(u64, u64), TapError>> =
            tokio::spawn(async move {
                    mysql_worker_main(
                    pool,
                    &table,
                    &pks,
                    assignment.rx,
                    event_tx,
                    lsn,
                    batch_size,
                )
                .await
            });
        handles.push((wi, handle));
    }

    // ── Step 6: Collect results ───────────────────────────────────────
    let mut total_rows: u64 = 0;
    let mut errors: Vec<(usize, String)> = Vec::new();

    for (wi, handle) in handles {
        match handle.await {
            Ok(Ok((rows, _chunks))) => {
                info!(worker = wi, rows, "worker finished");
                total_rows += rows;
            }
            Ok(Err(e)) => {
                error!(worker = wi, error = %e, "worker failed");
                errors.push((wi, e.to_string()));
            }
            Err(e) => {
                error!(worker = wi, error = %e, "worker panicked");
                errors.push((wi, format!("panic: {e}")));
            }
        }
    }

    if !errors.is_empty() {
        return Err(TapError::Snapshot(format!(
            "snapshot completed with {} worker error(s): {}",
            errors.len(),
            errors
                .into_iter()
                .map(|(w, e)| format!("worker {w}: {e}"))
                .collect::<Vec<_>>()
                .join("; "),
        )));
    }

    info!(
        snapshot_id,
        total_rows,
        tables = total_table_count,
        "MySQL parallel snapshot completed successfully",
    );

    Ok((snapshot_id, total_rows))
}

// ---------------------------------------------------------------------------
// Keeper protocol: FTWRL → SHOW MASTER STATUS → UNLOCK TABLES
// ---------------------------------------------------------------------------

/// Acquire FTWRL, capture binlog position, release lock.
async fn acquire_binlog_position(conn: &mut Conn) -> Result<(String, u64), TapError> {
    conn.query_drop("FLUSH TABLES WITH READ LOCK")
        .await
        .map_err(|e| TapError::Snapshot(format!("FTWRL failed: {e}")))?;

    // `SHOW MASTER STATUS` may fail — release lock on error.
    let rows = match show_master_status(conn).await {
        Ok(rows) => rows,
        Err(e) => {
            let _ = conn.query_drop("UNLOCK TABLES").await;
            return Err(e);
        }
    };

    let (file, offset) = match rows.into_iter().next() {
        Some(r) => {
            let file: String = r.get("File").unwrap_or_default();
            let offset: u64 = r.get("Position").unwrap_or(0);
            (file, offset)
        }
        None => {
            let _ = conn.query_drop("UNLOCK TABLES").await;
            return Err(TapError::Snapshot(
                "SHOW MASTER STATUS returned no rows — binary logging disabled?".into(),
            ));
        }
    };

    // Release lock immediately.
    conn.query_drop("UNLOCK TABLES")
        .await
        .map_err(|e| TapError::Snapshot(format!("UNLOCK TABLES failed: {e}")))?;

    info!(binlog_file = %file, binlog_offset = offset, "acquired binlog position");
    Ok((file, offset))
}

/// Run `SHOW MASTER STATUS` and return all rows.
async fn show_master_status(conn: &mut Conn) -> Result<Vec<MyRow>, TapError> {
    let mut result = conn
        .query_iter("SHOW MASTER STATUS")
        .await
        .map_err(|e| TapError::Snapshot(format!("SHOW MASTER STATUS failed: {e}")))?;

    result
        .collect()
        .await
        .map_err(|e| TapError::Snapshot(format!("collect SHOW MASTER STATUS failed: {e}")))
}

// ---------------------------------------------------------------------------
// Table resolution
// ---------------------------------------------------------------------------

fn resolve_tables(
    source_config: &MySqlSourceConfig,
    snap_config: &SnapshotConfig,
) -> Vec<TableInfo> {
    let table_names = if !snap_config.tables.is_empty() {
        &snap_config.tables
    } else {
        &source_config.tables
    };

    let dbname = &source_config.dbname;

    let mut tables: Vec<TableInfo> = table_names
        .iter()
        .map(|t| {
            let parts: Vec<&str> = t.split('.').collect();
            match parts.as_slice() {
                [name] => TableInfo {
                    schema: dbname.clone(),
                    name: name.to_string(),
                    qualified: format!("`{dbname}`.`{name}`"),
                },
                [schema, name] => TableInfo {
                    schema: schema.to_string(),
                    name: name.to_string(),
                    qualified: format!("`{schema}`.`{name}`"),
                },
                _ => TableInfo {
                    schema: dbname.clone(),
                    name: t.clone(),
                    qualified: format!("`{dbname}`.`{t}`"),
                },
            }
        })
        .collect();

    tables.sort_by(|a, b| a.qualified.cmp(&b.qualified));
    tables.dedup_by(|a, b| a.qualified == b.qualified);
    tables
}

// ---------------------------------------------------------------------------
// PK column detection (information_schema)
// ---------------------------------------------------------------------------

async fn mysql_detect_pk_columns(
    conn: &mut Conn,
    table: &TableInfo,
) -> Result<Vec<String>, TapError> {
    let rows: Vec<MyRow> = conn
        .exec_iter(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_KEY = 'PRI' \
             ORDER BY ORDINAL_POSITION",
            (&table.schema, &table.name),
        )
        .await
        .map_err(|e| {
            TapError::Snapshot(format!("PK query failed for {}: {e}", table.qualified))
        })?
        .collect()
        .await
        .map_err(|e| {
            TapError::Snapshot(format!("PK collect failed for {}: {e}", table.qualified))
        })?;

    Ok(rows
        .iter()
        .filter_map(|r| r.get::<String, &str>("COLUMN_NAME"))
        .collect())
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

async fn mysql_worker_main(
    pool: Arc<Pool>,
    table: &TableInfo,
    pks: &[String],
    mut chunk_rx: UnboundedReceiver<SnapshotChunk>,
    event_tx: Arc<UnboundedSender<ChangeEvent>>,
    lsn: Lsn,
    batch_size: u32,
) -> Result<(u64, u64), TapError> {
    let mut conn = pool
        .get_conn()
        .await
        .map_err(|e| TapError::Snapshot(format!("worker connect failed: {e}")))?;

    conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .await
        .map_err(|e| TapError::Snapshot(format!("START TRANSACTION failed: {e}")))?;

    let mut total_rows: u64 = 0;
    let mut chunks_processed: u64 = 0;

    while let Some(chunk) = chunk_rx.recv().await {
        let rows = mysql_scan_chunk(
            &mut conn,
            table,
            pks,
            &chunk,
            &event_tx,
            &lsn,
            batch_size,
        )
        .await?;
        total_rows += rows;
        chunks_processed += 1;
    }

    if let Err(e) = conn.query_drop("COMMIT").await {
        warn!(error = %e, "worker COMMIT failed (non-fatal)");
    }

    Ok((total_rows, chunks_processed))
}

// ---------------------------------------------------------------------------
// Chunk scanning
// ---------------------------------------------------------------------------

async fn mysql_scan_chunk(
    conn: &mut Conn,
    table: &TableInfo,
    pks: &[String],
    chunk: &SnapshotChunk,
    event_tx: &UnboundedSender<ChangeEvent>,
    lsn: &Lsn,
    batch_size: u32,
) -> Result<u64, TapError> {
    let where_clause = mysql_where_clause(pks, chunk);

    let order_clause: String = pks
        .iter()
        .map(|pk| format!("`{pk}`"))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "SELECT * FROM {} WHERE {} ORDER BY {} LIMIT {}",
        table.qualified, where_clause, order_clause, batch_size,
    );

    let mut result = conn.query_iter(sql.as_str()).await.map_err(|e| {
        TapError::Snapshot(format!(
            "scan failed for {} chunk {}: {e}",
            table.qualified, chunk.chunk_index,
        ))
    })?;

    let mut row_count: u64 = 0;

    loop {
        match result.next().await {
            Ok(Some(r)) => {
                emit_mysql_row_event(&r, table, event_tx, lsn)?;
                row_count += 1;
            }
            Ok(None) => break,
            Err(e) => {
                return Err(TapError::Snapshot(format!(
                    "row error in {} chunk {}: {e}",
                    table.qualified, chunk.chunk_index,
                )));
            }
        }
    }

    info!(
        table = table.qualified,
        chunk = chunk.chunk_index,
        rows = row_count,
        "scanned chunk",
    );

    Ok(row_count)
}

// ---------------------------------------------------------------------------
// MySQL WHERE clause from chunk bounds
// ---------------------------------------------------------------------------

fn mysql_where_clause(pks: &[String], chunk: &SnapshotChunk) -> String {
    let pk_col = &pks[0];
    match (&chunk.chunk_start, &chunk.chunk_end) {
        (Some(start), Some(end)) => {
            format!("`{pk_col}` >= {start} AND `{pk_col}` < {end}")
        }
        (Some(start), None) => {
            format!("`{pk_col}` >= {start}")
        }
        (None, Some(end)) => {
            format!("`{pk_col}` < {end}")
        }
        (None, None) => "TRUE".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Row emission
// ---------------------------------------------------------------------------

fn emit_mysql_row_event(
    row: &MyRow,
    table: &TableInfo,
    event_tx: &UnboundedSender<ChangeEvent>,
    lsn: &Lsn,
) -> Result<(), TapError> {
    let after = mysql_row_to_json_object(row);
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let source = SourceMetadata {
        db: table.schema.clone(),
        schema: String::new(),
        table: table.name.clone(),
        lsn: lsn.clone(),
        tx_id: "0".into(),
        ts_ms: now_ms,
        snapshot: Some(true),
    };

    let event = ChangeEventBuilder::new()
        .op(Operation::Read)
        .source(source)
        .after(Some(after))
        .build()
        .map_err(|e| TapError::Snapshot(format!("build ChangeEvent failed: {e}")))?;

    event_tx.send(event).map_err(|e| {
        TapError::Snapshot(format!("send ChangeEvent failed: {e}"))
    })?;

    Ok(())
}

// ---------------------------------------------------------------------------
// mysql_async Row → serde_json::Value
// ---------------------------------------------------------------------------

fn mysql_row_to_json_object(row: &MyRow) -> serde_json::Value {
    let cols = row.columns();

    let obj: serde_json::Map<String, serde_json::Value> = (0..cols.len())
        .filter_map(|i| {
            let name = cols[i].name_str().to_string();
            let val = row.as_ref(i)?;
            Some((name, mysql_value_to_json_value(val)))
        })
        .collect();

    serde_json::Value::Object(obj)
}

/// Safely extract a string representation of a column from a MySQL row.
fn mysql_get_string(row: &MyRow, column: &str) -> Option<String> {
    let cols = row.columns();
    for (i, col) in cols.iter().enumerate() {
        if col.name_str().as_ref() == column {
            let val = row.as_ref(i)?;
            return Some(mysql_value_to_string(val));
        }
    }
    None
}

fn mysql_value_to_string(val: &MyValue) -> String {
    match val {
        MyValue::NULL => String::new(),
        MyValue::Bytes(b) => String::from_utf8_lossy(b).to_string(),
        MyValue::Int(i) => i.to_string(),
        MyValue::UInt(u) => u.to_string(),
        MyValue::Float(f) => f.to_string(),
        MyValue::Date(y, mo, d, h, mi, s, us) => {
            format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{us:06}")
        }
        _ => format!("{val:?}"),
    }
}

fn mysql_value_to_json_value(val: &MyValue) -> serde_json::Value {
    match val {
        MyValue::NULL => serde_json::Value::Null,
        MyValue::Int(i) => serde_json::Value::Number((*i).into()),
        MyValue::UInt(u) => serde_json::Value::Number((*u).into()),
        MyValue::Float(f) => {
            if let Some(n) = serde_json::Number::from_f64(*f as f64) {
                serde_json::Value::Number(n)
            } else {
                serde_json::Value::String(f.to_string())
            }
        }
        MyValue::Bytes(b) => match std::str::from_utf8(b) {
            Ok(s) => serde_json::Value::String(s.to_string()),
            Err(_) => serde_json::Value::String(format!(
                "0x{}",
                b.iter().map(|b| format!("{b:02x}")).collect::<Vec<_>>().concat()
            )),
        },
        MyValue::Date(y, mo, d, h, mi, s, us) => {
            if *h == 0 && *mi == 0 && *s == 0 && *us == 0 {
                serde_json::Value::String(format!("{y:04}-{mo:02}-{d:02}"))
            } else {
                serde_json::Value::String(format!(
                    "{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{us:06}"
                ))
            }
        }
        _ => serde_json::Value::String(format!("{val:?}")),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::chunker::SnapshotChunk;

    // ── resolve_tables ─────────────────────────────────────────────────

    #[test]
    fn resolve_tables_prepends_dbname_when_bare_name() {
        let src = MySqlSourceConfig {
            host: "127.0.0.1".into(),
            port: 3306,
            dbname: "testdb".into(),
            user: "root".into(),
            password: "p".into(),
            tables: vec!["users".into(), "orders".into()],
            server_id: 1,
            binlog_file: None,
            binlog_offset: None,
        };
        let snap = SnapshotConfig {
            batch_size: 1000,
            num_workers: 4,
            tables: vec![],
        };

        let tables = resolve_tables(&src, &snap);

        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].schema, "testdb");
        assert_eq!(tables[0].name, "orders");
        assert_eq!(tables[0].qualified, "`testdb`.`orders`");
        assert_eq!(tables[1].name, "users");
    }

    #[test]
    fn resolve_tables_uses_snap_config_tables_when_given() {
        let src = MySqlSourceConfig {
            host: "127.0.0.1".into(),
            port: 3306,
            dbname: "testdb".into(),
            user: "root".into(),
            password: String::new(),
            tables: vec!["ignored".into()],
            server_id: 1,
            binlog_file: None,
            binlog_offset: None,
        };
        let snap = SnapshotConfig {
            batch_size: 1000,
            num_workers: 4,
            tables: vec!["mydb.customers".into()],
        };

        let tables = resolve_tables(&src, &snap);

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].schema, "mydb");
        assert_eq!(tables[0].name, "customers");
    }

    #[test]
    fn resolve_tables_dedupes_duplicates() {
        let src = MySqlSourceConfig {
            host: "127.0.0.1".into(),
            port: 3306,
            dbname: "testdb".into(),
            user: "root".into(),
            password: String::new(),
            tables: vec![],
            server_id: 1,
            binlog_file: None,
            binlog_offset: None,
        };
        let snap = SnapshotConfig {
            batch_size: 1000,
            num_workers: 4,
            tables: vec!["a".into(), "b".into(), "a".into()],
        };

        let tables = resolve_tables(&src, &snap);

        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn resolve_tables_returns_empty_when_no_tables() {
        let src = MySqlSourceConfig {
            host: "127.0.0.1".into(),
            port: 3306,
            dbname: "testdb".into(),
            user: "root".into(),
            password: String::new(),
            tables: vec![],
            server_id: 1,
            binlog_file: None,
            binlog_offset: None,
        };
        let snap = SnapshotConfig {
            batch_size: 1000,
            num_workers: 4,
            tables: vec![],
        };

        let tables = resolve_tables(&src, &snap);
        assert!(tables.is_empty());
    }

    // ── mysql_where_clause ─────────────────────────────────────────────

    #[test]
    fn where_clause_both_bounds() {
        let chunk = SnapshotChunk::new(
            "test.t".into(),
            "snap1".into(),
            0,
            Some("5".into()),
            Some("10".into()),
        );
        let sql = mysql_where_clause(&["id".into()], &chunk);
        assert_eq!(sql, "`id` >= 5 AND `id` < 10");
    }

    #[test]
    fn where_clause_only_start() {
        let chunk = SnapshotChunk::new(
            "test.t".into(),
            "snap1".into(),
            0,
            Some("100".into()),
            None,
        );
        let sql = mysql_where_clause(&["id".into()], &chunk);
        assert_eq!(sql, "`id` >= 100");
    }

    #[test]
    fn where_clause_only_end() {
        let chunk = SnapshotChunk::new(
            "test.t".into(),
            "snap1".into(),
            0,
            None,
            Some("50".into()),
        );
        let sql = mysql_where_clause(&["id".into()], &chunk);
        assert_eq!(sql, "`id` < 50");
    }

    #[test]
    fn where_clause_unbounded() {
        let chunk = SnapshotChunk::new(
            "test.t".into(),
            "snap1".into(),
            0,
            None,
            None,
        );
        let sql = mysql_where_clause(&["id".into()], &chunk);
        assert_eq!(sql, "TRUE");
    }

    // ── mysql_value_to_string ──────────────────────────────────────────

    #[test]
    fn value_to_string_null() {
        assert_eq!(mysql_value_to_string(&MyValue::NULL), "");
    }

    #[test]
    fn value_to_string_int() {
        assert_eq!(mysql_value_to_string(&MyValue::Int(42)), "42");
        assert_eq!(mysql_value_to_string(&MyValue::Int(-7)), "-7");
    }

    #[test]
    fn value_to_string_uint() {
        assert_eq!(mysql_value_to_string(&MyValue::UInt(999)), "999");
    }

    #[test]
    fn value_to_string_float() {
        let s = mysql_value_to_string(&MyValue::Float(3.14.into()));
        assert!(s.contains("3.14"), "got {s}");
    }

    #[test]
    fn value_to_string_bytes() {
        let s = mysql_value_to_string(&MyValue::Bytes(b"hello".to_vec()));
        assert_eq!(s, "hello");
    }

    #[test]
    fn value_to_string_date() {
        let s = mysql_value_to_string(&MyValue::Date(2024, 6, 13, 10, 30, 0, 0));
        assert_eq!(s, "2024-06-13T10:30:00.000000");
    }

    // ── mysql_value_to_json_value ──────────────────────────────────────

    #[test]
    fn value_to_json_null() {
        assert_eq!(mysql_value_to_json_value(&MyValue::NULL), serde_json::Value::Null);
    }

    #[test]
    fn value_to_json_int() {
        assert_eq!(mysql_value_to_json_value(&MyValue::Int(42)), serde_json::json!(42));
        assert_eq!(mysql_value_to_json_value(&MyValue::Int(-1)), serde_json::json!(-1));
    }

    #[test]
    fn value_to_json_uint() {
        assert_eq!(mysql_value_to_json_value(&MyValue::UInt(999)), serde_json::json!(999));
    }

    #[test]
    fn value_to_json_bytes() {
        assert_eq!(
            mysql_value_to_json_value(&MyValue::Bytes(b"hello".to_vec())),
            serde_json::json!("hello"),
        );
    }

    #[test]
    fn value_to_json_bytes_non_utf8() {
        // 0xFF is invalid UTF-8 → hex output
        let val = mysql_value_to_json_value(&MyValue::Bytes(vec![0xff, 0xfe]));
        assert!(val.is_string());
        assert!(val.as_str().unwrap().starts_with("0x"));
    }

    #[test]
    fn value_to_json_date_date_only() {
        let val = mysql_value_to_json_value(&MyValue::Date(2024, 12, 25, 0, 0, 0, 0));
        assert_eq!(val, serde_json::json!("2024-12-25"));
    }

    #[test]
    fn value_to_json_date_with_time() {
        let val = mysql_value_to_json_value(&MyValue::Date(2024, 6, 13, 14, 30, 0, 123456));
        assert_eq!(val, serde_json::json!("2024-06-13T14:30:00.123456"));
    }
}
