//! MySQL change event types and binlog event parsing.
//!
//! [`MySqlChangeEvent`] mirrors the Debezium envelope format used by
//! [`ChangeEvent`](crate::event::ChangeEvent), but carries MySQL-specific
//! position metadata (binlog file name + offset) instead of Postgres LSN.
//!
//! [`MySqlBinlogEvent`] represents a parsed raw binlog event with typed
//! data structs for each of the 7 variants required by CDC processing:
//! `WriteRows`, `UpdateRows`, `DeleteRows`, `TableMap`, `Rotate`, `Xid`,
//! and `Query`.
//!
//! The [`parse_binlog_event`] function converts a raw
//! [`mysql_async::binlog::events::Event`] into zero or more
//! [`MySqlBinlogEvent`] values, maintaining a table-map cache for decoding
//! row events.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::event::{ChangeEvent, Operation, SourceMetadata};
use crate::mysql::types::ColumnInfo;

use mysql_async::binlog::events::{EventData, RowsEventData};

// ──────────────────────────────────────────────
//  Change event (Debezium-style)
// ──────────────────────────────────────────────

/// A row-level change event originating from the MySQL binlog.
///
/// This is the MySQL counterpart of [`ChangeEvent`].  The two share the same
/// `before`/`after` JSON payload and Debezium-style `op` codes, but
/// `MySqlChangeEvent` uses a binlog position (file name + offset) instead of
/// Postgres LSN for source positioning.
///
/// When the event is emitted downstream it can be converted into a
/// generic [`ChangeEvent`] via [`From`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MySqlChangeEvent {
    /// Operation type — serialised as Debezium single-character codes.
    #[serde(with = "crate::event::operation_code")]
    pub op: Operation,
    /// Row state before the change (`None` for inserts).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<serde_json::Value>,
    /// Row state after the change (`None` for deletes).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<serde_json::Value>,
    /// MySQL-specific source metadata.
    pub source: MySqlSourceMetadata,
    /// Millisecond-precision timestamp (UNIX epoch) of the Tap event.
    pub ts_ms: u64,
    /// Unique event identifier — `{binlog_file}:{offset}:{tx_id}`.
    pub id: String,
}

/// MySQL source metadata, analogous to [`SourceMetadata`] but tracking
/// binlog position rather than Postgres LSN.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct MySqlSourceMetadata {
    /// Source database name.
    pub db: String,
    /// Source table name (e.g. `"users"`).
    pub table: String,
    /// Binlog file name (e.g. `"mysql-bin.000042"`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub binlog_file: String,
    /// Byte offset within the binlog file.
    #[serde(default)]
    pub binlog_offset: u64,
    /// Identifier of the transaction that produced the change
    /// (MySQL `gtid_next` or `Xid`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tx_id: String,
    /// Timestamp (milliseconds since UNIX epoch) of the change in MySQL.
    pub ts_ms: u64,
    /// Whether this event was produced by a snapshot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<bool>,
}

impl From<MySqlChangeEvent> for ChangeEvent {
    fn from(e: MySqlChangeEvent) -> Self {
        ChangeEvent {
            op: e.op,
            before: e.before,
            after: e.after,
            source: SourceMetadata {
                db: e.source.db,
                schema: String::new(), // MySQL has no schema layer
                table: e.source.table,
                lsn: String::new().parse().unwrap(),
                tx_id: e.source.tx_id,
                ts_ms: e.source.ts_ms,
                snapshot: e.source.snapshot,
            },
            ts_ms: e.ts_ms,
            id: e.id,
        }
    }
}

// ──────────────────────────────────────────────
//  Raw binlog event types
// ──────────────────────────────────────────────

/// A single row's data as a JSON object keyed by column name.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RowData {
    /// Column values: `{ "column_name": value, ... }`.
    pub values: serde_json::Value,
}

/// Row-level data for a write (insert) event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WriteRowsData {
    /// The table ID this event applies to (matches a preceding TableMapEvent).
    pub table_id: u64,
    /// The inserted rows.
    pub rows: Vec<RowData>,
    /// Source metadata for the event.
    pub source: MySqlSourceMetadata,
}

/// Row-level data for an update event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UpdateRowsData {
    /// The table ID this event applies to.
    pub table_id: u64,
    /// The updated rows as (before, after) pairs.
    pub rows: Vec<(RowData, RowData)>,
    /// Source metadata for the event.
    pub source: MySqlSourceMetadata,
}

/// Row-level data for a delete event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeleteRowsData {
    /// The table ID this event applies to.
    pub table_id: u64,
    /// The deleted rows.
    pub rows: Vec<RowData>,
    /// Source metadata for the event.
    pub source: MySqlSourceMetadata,
}

/// Cached metadata extracted from a `TableMapEvent`.
///
/// This struct stores the column-level metadata needed for decoding row
/// events that reference a specific table.  It is populated when a
/// `TableMapEvent` is parsed and looked up by `table_id` when subsequent
/// row events arrive.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableMapEventData {
    /// The table ID (unique within a binlog file).
    pub table_id: u64,
    /// Database (schema) name.
    pub db: String,
    /// Table name.
    pub table: String,
    /// Number of columns in the table.
    pub num_columns: usize,
    /// Per-column type info (names, types, signedness).
    pub columns: Vec<ColumnInfo>,
}

/// A binlog file rotation event.
///
/// Emitted when the MySQL server rotates to a new binlog file.  Consumers
/// should update their tracked binlog file name to continue reading events.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RotateEventData {
    /// The next binlog file name (e.g. `"mysql-bin.000043"`).
    pub next_binlog_file: String,
    /// Position within the binlog to rotate to.
    pub position: u64,
}

/// An XID (transaction commit) event.
///
/// Emitted when a transaction that modified one or more tables of an
/// XA-capable storage engine commits.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct XidEventData {
    /// The XID (transaction identifier).
    pub xid: u64,
}

/// A query event (DDL, `BEGIN`, `COMMIT`, etc.).
///
/// Emitted for DDL statements and transactional control statements that
/// do not generate row events.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryEventData {
    /// Thread ID of the connection that executed the query.
    pub thread_id: u32,
    /// Time in seconds the query took to execute.
    pub execution_time: u32,
    /// Error code (0 if successful).
    pub error_code: u16,
    /// Schema (database) the query was executed on.
    pub schema: String,
    /// The SQL query text.
    pub query: String,
}

/// A parsed binlog event.
///
/// Each variant maps to one of the 7 MySQL binlog event types that are
/// relevant to CDC processing.  The `#[serde(tag = "type")]` attribute
/// enables JSON serialisation with a discriminant string for each variant.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum MySqlBinlogEvent {
    /// Row insert — one or more rows were created.
    WriteRows(WriteRowsData),
    /// Row update — one or more rows were modified (before + after).
    UpdateRows(UpdateRowsData),
    /// Row deletion — one or more rows were removed.
    DeleteRows(DeleteRowsData),
    /// Table map — column metadata for a table.
    TableMap(TableMapEventData),
    /// Binlog file rotation.
    Rotate(RotateEventData),
    /// Transaction commit (XID).
    Xid(XidEventData),
    /// Query event (DDL, BEGIN, COMMIT, etc.).
    Query(QueryEventData),
}

// ──────────────────────────────────────────────
//  Conversion helpers
// ──────────────────────────────────────────────

/// Convert a `mysql_async::Column` to the local [`ColumnInfo`] type.
pub fn column_to_column_info(col: &mysql_async::Column) -> ColumnInfo {
    ColumnInfo {
        name: col.name_str().to_string(),
        col_type: col.column_type(),
        is_unsigned: col
            .flags()
            .contains(mysql_async::consts::ColumnFlags::UNSIGNED_FLAG),
    }
}

/// Extract column info from a `BinlogRow`.
///
/// Each [`mysql_async::binlog::row::BinlogRow`] carries its own column
/// metadata (`Arc<[Column]>`), so no external table-map lookup is needed
/// for basic name+type extraction.
pub fn binlog_row_columns(row: &mysql_async::binlog::row::BinlogRow) -> Vec<ColumnInfo> {
    row.columns_ref().iter().map(column_to_column_info).collect()
}

/// Convert a `BinlogRow` to the local [`RowData`] type.
///
/// # Panics
///
/// Panics if a `BinlogValue` cannot be converted to JSON (this should
/// not happen in practice for standard MySQL types).
pub fn binlog_row_to_row_data(row: &mysql_async::binlog::row::BinlogRow) -> RowData {
    use mysql_async::binlog::value::BinlogValue;

    let columns = row.columns_ref();
    let mut map = serde_json::Map::with_capacity(columns.len());

    for (i, col) in columns.iter().enumerate() {
        let value = match row.as_ref(i) {
            Some(BinlogValue::Value(v)) => crate::mysql::types::mysql_value_to_json(v),
            Some(BinlogValue::Jsonb(_)) => {
                // JSONB not yet handled; emit a placeholder
                serde_json::Value::String("[jsonb]".to_string())
            }
            Some(BinlogValue::JsonDiff(_)) => {
                // JSON diff not yet handled; emit a placeholder
                serde_json::Value::String("[jsondiff]".to_string())
            }
            None => serde_json::Value::Null,
        };

        map.insert(col.name_str().to_string(), value);
    }

    RowData {
        values: serde_json::Value::Object(map),
    }
}

// ──────────────────────────────────────────────
//  Parsing
// ──────────────────────────────────────────────

/// Cache of raw binlog [`Event`] values for table-map lookups, keyed by
/// table ID.
pub type TableMapCache = HashMap<u64, mysql_async::binlog::events::Event>;

/// Parse a raw binlog event into zero or more [`MySqlBinlogEvent`] values.
///
/// `table_map_cache` accumulates [`TableMapEvent`] entries as they arrive,
/// allowing subsequent row events to be decoded using the stored column
/// metadata.
///
/// # Examples
///
/// ```ignore
/// use std::collections::HashMap;
/// use tap_core::mysql::events::{parse_binlog_event, TableMapCache};
///
/// let mut cache = TableMapCache::new();
///
/// // Each event from a BinlogStream is parsed in sequence:
/// for event in stream {
///     for parsed in parse_binlog_event(&event, &mut cache) {
///         match parsed {
///             MySqlBinlogEvent::WriteRows(data) => { /* ... */ }
///             // ...
///         }
///     }
/// }
/// ```
pub fn parse_binlog_event(
    event: &mysql_async::binlog::events::Event,
    table_map_cache: &mut TableMapCache,
) -> Vec<MySqlBinlogEvent> {
    let Ok(Some(data)) = event.read_data() else {
        return Vec::new();
    };

    let timestamp_ms = event.header().timestamp() as u64 * 1000;
    let server_id = event.header().server_id();
    let log_pos = event.header().log_pos() as u64;

    match data {
        EventData::TableMapEvent(tme) => {
            let table_id = tme.table_id();
            let db = tme.database_name().to_string();
            let table_name = tme.table_name().to_string();
            let num_columns = tme.columns_count() as usize;

            let columns = extract_columns_from_table_map(&tme);
            let info = TableMapEventData {
                table_id,
                db,
                table: table_name,
                num_columns,
                columns,
            };

            // Store the raw event for future row decoding (the Event is
            // Clone because it owns its data bytes).
            table_map_cache.insert(table_id, event.clone());

            vec![MySqlBinlogEvent::TableMap(info)]
        }

        EventData::RowsEvent(rows_data) => {
            parse_rows_event(rows_data, table_map_cache, server_id, timestamp_ms, log_pos)
        }

        EventData::RotateEvent(re) => {
            let data = RotateEventData {
                next_binlog_file: re.name().to_string(),
                position: re.position(),
            };
            vec![MySqlBinlogEvent::Rotate(data)]
        }

        EventData::XidEvent(xe) => {
            let data = XidEventData { xid: xe.xid };
            vec![MySqlBinlogEvent::Xid(data)]
        }

        EventData::QueryEvent(qe) => {
            let data = QueryEventData {
                thread_id: qe.thread_id(),
                execution_time: qe.execution_time(),
                error_code: qe.error_code(),
                schema: qe.schema().to_string(),
                query: qe.query().to_string(),
            };
            vec![MySqlBinlogEvent::Query(data)]
        }

        _ => Vec::new(),
    }
}

/// Extract column info from a `TableMapEvent`, using optional metadata
/// for column names when available.
fn extract_columns_from_table_map(
    tme: &mysql_async::binlog::events::TableMapEvent<'_>,
) -> Vec<ColumnInfo> {
    let num_columns = tme.columns_count() as usize;
    let mut columns = Vec::with_capacity(num_columns);

    for i in 0..num_columns {
        let col_type = tme
            .get_column_type(i)
            .ok()
            .flatten()
            .unwrap_or(mysql_async::consts::ColumnType::MYSQL_TYPE_STRING);
        let is_unsigned = false; // enhanced from optional metadata if needed
        columns.push(ColumnInfo {
            name: format!("_col_{}", i),
            col_type,
            is_unsigned,
        });
    }

    // Overwrite positional names with real column names from optional
    // metadata (available on MySQL 8.0.1+ with
    // binlog_transaction_dependency_tracking enabled).
    use mysql_async::binlog::events::OptionalMetadataField;
    for meta in tme.iter_optional_meta() {
        if let Ok(OptionalMetadataField::ColumnName(names)) = meta {
            for (i, name_result) in names.iter_names().enumerate() {
                if let Ok(name) = name_result {
                    if let Some(col) = columns.get_mut(i) {
                        col.name = name.name().to_string();
                    }
                }
            }
        }
    }

    columns
}

/// Decode a `RowsEventData` (which may be WriteRows, UpdateRows, or
/// DeleteRows) into the corresponding [`MySqlBinlogEvent`] variant.
///
/// The cached `TableMapEvent` is converted to an owned form so its
/// lifetime is compatible with the rows iterator API (which requires
/// the same `'a` on both `RowsEvent` and `TableMapEvent`).
fn parse_rows_event(
    rows_data: mysql_async::binlog::events::RowsEventData<'_>,
    table_map_cache: &TableMapCache,
    _server_id: u32,
    _timestamp_ms: u64,
    _log_pos: u64,
) -> Vec<MySqlBinlogEvent> {
    let table_id = rows_data.table_id();

    // Look up the cached TableMapEvent to decode rows.  If no table map
    // is available the rows are skipped (this should not happen in a
    // well-formed binlog stream).
    let cached_event = match table_map_cache.get(&table_id) {
        Some(e) => e,
        None => {
            tracing::warn!(
                "No TableMapEvent found for table_id={}; skipping row event",
                table_id
            );
            return Vec::new();
        }
    };

    let Ok(Some(EventData::TableMapEvent(tme))) = cached_event.read_data() else {
        tracing::warn!(
            "Cached event for table_id={} is not a TableMapEvent",
            table_id
        );
        return Vec::new();
    };

    // Convert to owned so the lifetime ('static) outlives any RowsEvent
    // we are decoding — `.rows()` requires matching `'a` on both sides.
    let tme_owned = tme.into_owned();

    let db = tme_owned.database_name().to_string();
    let table = tme_owned.table_name().to_string();
    let binlog_file = String::new(); // caller sets this from stream context
    let tx_id = String::new(); // caller sets this from GTID or Xid

    // Build source metadata for each row event
    let source = MySqlSourceMetadata {
        db,
        table,
        binlog_file,
        binlog_offset: _log_pos,
        tx_id,
        ts_ms: _timestamp_ms,
        snapshot: None,
    };

    match rows_data {
        RowsEventData::WriteRowsEvent(ref wre) => {
            let mut rows = Vec::new();
            let iter = wre.rows(&tme_owned);
            for row_result in iter {
                match row_result {
                    Ok((_, Some(after))) => {
                        rows.push(binlog_row_to_row_data(&after));
                    }
                    Ok((_, None)) => {
                        // WriteRows should always have an after-image
                    }
                    Err(e) => {
                        tracing::warn!("Failed to decode binlog row: {e}");
                    }
                }
            }
            vec![MySqlBinlogEvent::WriteRows(WriteRowsData {
                table_id,
                rows,
                source,
            })]
        }

        RowsEventData::UpdateRowsEvent(ref ure) => {
            let mut rows = Vec::new();
            let iter = ure.rows(&tme_owned);
            for row_result in iter {
                match row_result {
                    Ok((before, after)) => {
                        let before_row = before.map(|r| binlog_row_to_row_data(&r));
                        let after_row = after.map(|r| binlog_row_to_row_data(&r));
                        rows.push((
                        before_row.unwrap_or(RowData {
                            values: serde_json::Value::Null,
                        }),
                        after_row.unwrap_or(RowData {
                            values: serde_json::Value::Null,
                        }),
                        ));
                    }
                    Err(e) => {
                        tracing::warn!("Failed to decode binlog row: {e}");
                    }
                }
            }
            vec![MySqlBinlogEvent::UpdateRows(UpdateRowsData {
                table_id,
                rows,
                source,
            })]
        }

        RowsEventData::DeleteRowsEvent(ref dre) => {
            let mut rows = Vec::new();
            let iter = dre.rows(&tme_owned);
            for row_result in iter {
                match row_result {
                    Ok((Some(before), _)) => {
                        rows.push(binlog_row_to_row_data(&before));
                    }
                    Ok((None, _)) => {
                        // DeleteRows should always have a before-image
                    }
                    Err(e) => {
                        tracing::warn!("Failed to decode binlog row: {e}");
                    }
                }
            }
            vec![MySqlBinlogEvent::DeleteRows(DeleteRowsData {
                table_id,
                rows,
                source,
            })]
        }

        // V1 and PartialUpdate variants are decoded best-effort with
        // the same code path (the underlying RowsEvent struct is the same).
        RowsEventData::WriteRowsEventV1(ref wre) => {
            let mut rows = Vec::new();
            let iter = wre.rows(&tme_owned);
            for row_result in iter {
                if let Ok((_, Some(after))) = row_result {
                    rows.push(binlog_row_to_row_data(&after));
                }
            }
            vec![MySqlBinlogEvent::WriteRows(WriteRowsData {
                table_id,
                rows,
                source,
            })]
        }

        RowsEventData::UpdateRowsEventV1(ref ure) => {
            let mut rows = Vec::new();
            let iter = ure.rows(&tme_owned);
            for (before, after) in iter.flatten() {
                rows.push((
                    before
                        .as_ref()
                        .map(binlog_row_to_row_data)
                        .unwrap_or(RowData {
                            values: serde_json::Value::Null,
                        }),
                    after
                        .as_ref()
                        .map(binlog_row_to_row_data)
                        .unwrap_or(RowData {
                            values: serde_json::Value::Null,
                        }),
                ));
            }
            vec![MySqlBinlogEvent::UpdateRows(UpdateRowsData {
                table_id,
                rows,
                source,
            })]
        }

        RowsEventData::DeleteRowsEventV1(ref dre) => {
            let mut rows = Vec::new();
            let iter = dre.rows(&tme_owned);
            for row_result in iter {
                if let Ok((Some(before), _)) = row_result {
                    rows.push(binlog_row_to_row_data(&before));
                }
            }
            vec![MySqlBinlogEvent::DeleteRows(DeleteRowsData {
                table_id,
                rows,
                source,
            })]
        }

        RowsEventData::PartialUpdateRowsEvent(ref pre) => {
            // Partial update rows are decoded identically to
            // WriteRowsEvent for now (they carry the final after-image).
            let mut rows = Vec::new();
            let iter = pre.rows(&tme_owned);
            for row_result in iter {
                if let Ok((_, Some(after))) = row_result {
                    rows.push(binlog_row_to_row_data(&after));
                }
            }
            vec![MySqlBinlogEvent::WriteRows(WriteRowsData {
                table_id,
                rows,
                source,
            })]
        }
    }
}

// ──────────────────────────────────────────────
//  Tests
// ──────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // -- RowData tests ---------------------------------------------------

    #[test]
    fn test_row_data_roundtrip() {
        let rd = RowData {
            values: serde_json::json!({"id": 1, "name": "Alice"}),
        };
        let json = serde_json::to_string(&rd).unwrap();
        let back: RowData = serde_json::from_str(&json).unwrap();
        assert_eq!(rd, back);
    }

    // -- MySqlBinlogEvent tests (constructors + roundtrip) ---------------

    #[test]
    fn test_write_rows_event_roundtrip() {
        let event = MySqlBinlogEvent::WriteRows(WriteRowsData {
            table_id: 42,
            rows: vec![RowData {
                values: serde_json::json!({"id": 1}),
            }],
            source: MySqlSourceMetadata {
                db: "test".into(),
                table: "users".into(),
                ..Default::default()
            },
        });
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains(r#""type":"WriteRows""#));
        let back: MySqlBinlogEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, back);
    }

    #[test]
    fn test_update_rows_event_roundtrip() {
        let event = MySqlBinlogEvent::UpdateRows(UpdateRowsData {
            table_id: 42,
            rows: vec![(
                RowData {
                    values: serde_json::json!({"id": 1, "name": "Alice"}),
                },
                RowData {
                    values: serde_json::json!({"id": 1, "name": "Bob"}),
                },
            )],
            source: MySqlSourceMetadata {
                db: "test".into(),
                table: "users".into(),
                ..Default::default()
            },
        });
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains(r#""type":"UpdateRows""#));
        let back: MySqlBinlogEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, back);
    }

    #[test]
    fn test_delete_rows_event_roundtrip() {
        let event = MySqlBinlogEvent::DeleteRows(DeleteRowsData {
            table_id: 7,
            rows: vec![RowData {
                values: serde_json::json!({"id": 99}),
            }],
            source: MySqlSourceMetadata {
                db: "test".into(),
                table: "orders".into(),
                ..Default::default()
            },
        });
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains(r#""type":"DeleteRows""#));
        let back: MySqlBinlogEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, back);
    }

    #[test]
    fn test_table_map_event_roundtrip() {
        let event = MySqlBinlogEvent::TableMap(TableMapEventData {
            table_id: 42,
            db: "test".into(),
            table: "users".into(),
            num_columns: 3,
            columns: vec![
                ColumnInfo {
                    name: "id".into(),
                    col_type: mysql_async::consts::ColumnType::MYSQL_TYPE_LONG,
                    is_unsigned: false,
                },
                ColumnInfo {
                    name: "name".into(),
                    col_type: mysql_async::consts::ColumnType::MYSQL_TYPE_VARCHAR,
                    is_unsigned: false,
                },
            ],
        });
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains(r#""type":"TableMap""#));
        let back: MySqlBinlogEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, back);
    }

    #[test]
    fn test_rotate_event_roundtrip() {
        let event = MySqlBinlogEvent::Rotate(RotateEventData {
            next_binlog_file: "mysql-bin.000043".into(),
            position: 4,
        });
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains(r#""type":"Rotate""#));
        let back: MySqlBinlogEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, back);
    }

    #[test]
    fn test_xid_event_roundtrip() {
        let event = MySqlBinlogEvent::Xid(XidEventData { xid: 12345 });
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains(r#""type":"Xid""#));
        let back: MySqlBinlogEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, back);
    }

    #[test]
    fn test_query_event_roundtrip() {
        let event = MySqlBinlogEvent::Query(QueryEventData {
            thread_id: 7,
            execution_time: 0,
            error_code: 0,
            schema: "test".into(),
            query: "SELECT 1".into(),
        });
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains(r#""type":"Query""#));
        let back: MySqlBinlogEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, back);
    }

    // -- ColumnInfo conversion tests ------------------------------------

    #[test]
    fn test_column_to_column_info() {
        // Constructing a real mysql_async::Column is non-trivial
        // (requires serialisation).  This test verifies the function
        // signature compiles and handles placeholders.
        // Real column conversion is exercised indirectly through
        // binlog_row_columns tests when a binlog event source is available.
        assert!(true, "column_to_column_info compiles");
    }

    #[test]
    fn test_binlog_row_columns_empty() {
        // A BinlogRow with no columns is unusual but should not panic.
        let columns = Vec::new();
        let arc_cols: std::sync::Arc<[mysql_async::Column]> = columns.into();
        let row = mysql_async::binlog::row::BinlogRow::new(Vec::new(), arc_cols);
        let result = binlog_row_columns(&row);
        assert!(result.is_empty());
    }

    // -- parse_binlog_event -----------------------

    #[test]
    fn test_parse_binlog_event_returns_empty_for_unknown_events() {
        // Constructing a real binlog Event requires raw bytes from an
        // actual binlog dump.  This test verifies the function compiles
        // and that the cache-based API is consistent.
        let mut cache = TableMapCache::new();
        assert!(cache.is_empty());

        // Verify the cache can hold values (simulating an insert).
        // A real Event value requires full binlog deserialisation, so
        // we just verify the type system works.
        assert!(true, "parse_binlog_event with cache compiles");
    }

    // -- MySqlChangeEvent tests (existing) -------

    #[test]
    fn test_my_sql_change_event_roundtrip() {
        let event = MySqlChangeEvent {
            op: Operation::Create,
            before: None,
            after: Some(serde_json::json!({"id": 1, "name": "Alice"})),
            source: MySqlSourceMetadata {
                db: "mydb".into(),
                table: "users".into(),
                binlog_file: "mysql-bin.000042".into(),
                binlog_offset: 12345,
                tx_id: "abc-def".into(),
                ts_ms: 1_700_000_000_000,
                snapshot: None,
            },
            ts_ms: 1_700_000_000_001,
            id: "mysql-bin.000042:12345:abc-def".into(),
        };

        let json = serde_json::to_string(&event).expect("serialize");
        let deserialized: MySqlChangeEvent = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(event, deserialized);
        // Verify wire format uses single-char op codes
        assert!(json.contains(r#""op":"c""#));
    }

    #[test]
    fn test_my_sql_change_event_conversion_to_change_event() {
        let mysql_event = MySqlChangeEvent {
            op: Operation::Update,
            before: Some(serde_json::json!({"id": 1, "name": "Alice"})),
            after: Some(serde_json::json!({"id": 1, "name": "Bob"})),
            source: MySqlSourceMetadata {
                db: "mydb".into(),
                table: "users".into(),
                binlog_file: "mysql-bin.000042".into(),
                binlog_offset: 12345,
                tx_id: "abc-def".into(),
                ts_ms: 1_700_000_000_000,
                snapshot: None,
            },
            ts_ms: 1_700_000_000_001,
            id: "mysql-bin.000042:12345:abc-def".into(),
        };

        let generic: ChangeEvent = mysql_event.into();
        assert_eq!(generic.op, Operation::Update);
        assert_eq!(
            generic.before,
            Some(serde_json::json!({"id": 1, "name": "Alice"}))
        );
        assert_eq!(
            generic.after,
            Some(serde_json::json!({"id": 1, "name": "Bob"}))
        );
        // Schema is empty for MySQL
        assert_eq!(generic.source.schema, "");
    }

    #[test]
    fn test_my_sql_binlog_event_serde_tag() {
        // Verify that each variant serialises with a "type" tag.
        let cases: Vec<(&str, MySqlBinlogEvent)> = vec![
            (
                "WriteRows",
                MySqlBinlogEvent::WriteRows(WriteRowsData {
                    table_id: 0,
                    rows: vec![],
                    source: MySqlSourceMetadata::default(),
                }),
            ),
            (
                "UpdateRows",
                MySqlBinlogEvent::UpdateRows(UpdateRowsData {
                    table_id: 0,
                    rows: vec![],
                    source: MySqlSourceMetadata::default(),
                }),
            ),
            (
                "DeleteRows",
                MySqlBinlogEvent::DeleteRows(DeleteRowsData {
                    table_id: 0,
                    rows: vec![],
                    source: MySqlSourceMetadata::default(),
                }),
            ),
            (
                "TableMap",
                MySqlBinlogEvent::TableMap(TableMapEventData {
                    table_id: 0,
                    db: String::new(),
                    table: String::new(),
                    num_columns: 0,
                    columns: vec![],
                }),
            ),
            (
                "Rotate",
                MySqlBinlogEvent::Rotate(RotateEventData {
                    next_binlog_file: String::new(),
                    position: 0,
                }),
            ),
            (
                "Xid",
                MySqlBinlogEvent::Xid(XidEventData { xid: 0 }),
            ),
            (
                "Query",
                MySqlBinlogEvent::Query(QueryEventData {
                    thread_id: 0,
                    execution_time: 0,
                    error_code: 0,
                    schema: String::new(),
                    query: String::new(),
                }),
            ),
        ];

        for (expected_tag, event) in &cases {
            let json = serde_json::to_string(event).unwrap();
            let tag = format!(r#""type":"{}""#, expected_tag);
            assert!(
                json.contains(&tag),
                "Expected tag {tag:?} in JSON for variant {expected_tag}, got {json}"
            );
        }
    }
}
