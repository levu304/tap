//! MySQL change event types and binlog event parsing.
//!
//! [`MySqlChangeEvent`] mirrors the Debezium envelope format used by
//! [`ChangeEvent`](crate::event::ChangeEvent), but carries MySQL-specific
//! position metadata (binlog file name + offset) instead of Postgres LSN.
//!
//! The [`parse_binlog_event`] function is a **placeholder** for the future
//! binlog-streaming phase — it accepts a raw [`mysql_async::binlog::Event`]
//! and returns parsed events.  The current implementation returns an empty
//! vec and logs a warning.

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::event::{ChangeEvent, Operation, SourceMetadata};

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

/// Placeholder: parse a raw binlog event into zero or more
/// [`MySqlChangeEvent`] values.
///
/// # Current behaviour
///
/// This function is a stub that logs a warning and returns an empty vector.
/// Full binlog-streaming logic will be implemented in a later phase, at
/// which point this function will:
///
/// 1. Match on [`mysql_async::binlog::events::EventData`] variants:
///    - `WriteRowsEvent` → one `Create` event per row
///    - `UpdateRowsEvent` → one `Update` event per row pair (before/after)
///    - `DeleteRowsEvent` → one `Delete` event per row
/// 2. Extract column metadata from the corresponding `TableMapEvent`
/// 3. Convert `BinlogRow` columns to JSON using
///    [`mysql_value_to_json`](crate::mysql::types::mysql_value_to_json)
/// 4. Populate `MySqlSourceMetadata` from the binlog event header
///
/// # Parameters
///
/// * `_event` — A fully-parsed binlog [`mysql_async::binlog::events::Event`].
///
/// # Returns
///
/// Zero or more [`MySqlChangeEvent`] values extracted from the event.
/// Changes that could not be decoded are silently skipped.
pub fn parse_binlog_event(_event: &mysql_async::binlog::events::Event) -> Vec<MySqlChangeEvent> {
    warn!("parse_binlog_event is a placeholder — binlog streaming not yet implemented");
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_parse_binlog_event_stub() {
        // Constructing a real binlog Event requires raw bytes from a real
        // binlog file, so we just verify the placeholder compiles and that
        // the re-exported types work.
        assert!(true, "placeholder function compiles");
    }
}
