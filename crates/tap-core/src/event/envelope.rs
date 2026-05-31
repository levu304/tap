//! Core event types — `ChangeEvent`, `SourceMetadata`, and `Operation`.

use serde::{Deserialize, Serialize};

use crate::error::TapError;

/// A single data-change event, modelled after the Debezium envelope format.
///
/// This struct represents one row-level change captured from a Postgres
/// replication stream.  The `op` field is stored as a plain `String` to
/// match the Debezium wire format; use [`Operation`] for typed matching.
///
/// # Examples
///
/// ```
/// use tap_core::event::{ChangeEvent, Operation, SourceMetadata};
///
/// let source = SourceMetadata {
///     db: "mydb".into(),
///     schema: "public".into(),
///     table: "users".into(),
///     lsn: "0/1234567".into(),
///     tx_id: "12345".into(),
///     ts_ms: 1_700_000_000_000,
///     snapshot: None,
/// };
///
/// let event = ChangeEvent {
///     op: Operation::Create.as_str().to_string(),
///     before: None,
///     after: Some(serde_json::json!({"id": 1, "name": "Alice"})),
///     source,
///     ts_ms: 1_700_000_000_001,
///     id: "0/1234567:12345".into(),
/// };
///
/// assert_eq!(event.op, "c");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChangeEvent {
    /// Operation type: `"c"` (create), `"u"` (update), `"d"` (delete), `"r"` (read/snapshot).
    pub op: String,
    /// Row state before the change (None for inserts).
    pub before: Option<serde_json::Value>,
    /// Row state after the change (None for deletes).
    pub after: Option<serde_json::Value>,
    /// Metadata describing the source database transaction.
    pub source: SourceMetadata,
    /// Millisecond-precision timestamp (UNIX epoch) of the Tap event.
    pub ts_ms: u64,
    /// Unique event identifier — `{lsn}:{tx_id}` for streaming events,
    /// `snap:{schema}.{table}:{uuid}` for snapshot events.
    pub id: String,
}

/// Metadata describing the origin of a change event.
///
/// Mirrors the `source` block of a Debezium message so downstream
/// consumers can map events back to a specific database, schema,
/// table, and Postgres LSN position.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceMetadata {
    /// Source database name.
    pub db: String,
    /// Source schema name.
    pub schema: String,
    /// Source table name.
    pub table: String,
    /// Postgres WAL Log Sequence Number (e.g. `"0/1234567"`).
    pub lsn: String,
    /// Identifier of the transaction that produced the change.
    pub tx_id: String,
    /// Timestamp (milliseconds since UNIX epoch) of the change in Postgres.
    pub ts_ms: u64,
    /// Whether this event was produced by a snapshot.  `None` or `Some(false)`
    /// means streaming replication.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<bool>,
}

/// Typed representation of a CDC operation.
///
/// Maps to the single-character Debezium operation codes:
///
/// | Code | Variant   | Meaning          |
/// |------|-----------|------------------|
/// | `c`  | `Create`  | Row inserted     |
/// | `u`  | `Update`  | Row updated      |
/// | `d`  | `Delete`  | Row deleted      |
/// | `r`  | `Read`    | Snapshot read    |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Create,
    Update,
    Delete,
    Read,
}

impl Operation {
    /// Returns the single-character Debezium operation code.
    ///
    /// # Examples
    ///
    /// ```
    /// use tap_core::event::Operation;
    ///
    /// assert_eq!(Operation::Create.as_str(), "c");
    /// assert_eq!(Operation::Update.as_str(), "u");
    /// assert_eq!(Operation::Delete.as_str(), "d");
    /// assert_eq!(Operation::Read.as_str(), "r");
    /// ```
    pub fn as_str(&self) -> &'static str {
        match self {
            Operation::Create => "c",
            Operation::Update => "u",
            Operation::Delete => "d",
            Operation::Read => "r",
        }
    }

    /// Parses a single-character operation code into an `Operation`.
    ///
    /// # Errors
    ///
    /// Returns [`TapError::Config`] when the string is not one of
    /// `"c"`, `"u"`, `"d"`, or `"r"`.
    ///
    /// # Examples
    ///
    /// ```
    /// use tap_core::event::Operation;
    ///
    /// assert_eq!(Operation::from_str("c").unwrap(), Operation::Create);
    /// assert!(Operation::from_str("x").is_err());
    /// ```
    pub fn from_str(s: &str) -> Result<Self, TapError> {
        match s {
            "c" => Ok(Operation::Create),
            "u" => Ok(Operation::Update),
            "d" => Ok(Operation::Delete),
            "r" => Ok(Operation::Read),
            other => Err(TapError::Config(format!(
                "invalid operation code: {other:?} (expected c/u/d/r)"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_as_str() {
        assert_eq!(Operation::Create.as_str(), "c");
        assert_eq!(Operation::Update.as_str(), "u");
        assert_eq!(Operation::Delete.as_str(), "d");
        assert_eq!(Operation::Read.as_str(), "r");
    }

    #[test]
    fn test_operation_from_str_valid() {
        assert_eq!(Operation::from_str("c").unwrap(), Operation::Create);
        assert_eq!(Operation::from_str("u").unwrap(), Operation::Update);
        assert_eq!(Operation::from_str("d").unwrap(), Operation::Delete);
        assert_eq!(Operation::from_str("r").unwrap(), Operation::Read);
    }

    #[test]
    fn test_operation_from_str_invalid() {
        let err = Operation::from_str("x").unwrap_err();
        assert!(err.to_string().contains("invalid operation code"));
    }

    #[test]
    fn test_event_roundtrip_json() {
        let source = SourceMetadata {
            db: "test_db".into(),
            schema: "public".into(),
            table: "users".into(),
            lsn: "0/ABCDEF".into(),
            tx_id: "42".into(),
            ts_ms: 1_700_000_000_000,
            snapshot: None,
        };

        let event = ChangeEvent {
            op: Operation::Create.as_str().to_string(),
            before: None,
            after: Some(serde_json::json!({"id": 1, "name": "Alice"})),
            source: source.clone(),
            ts_ms: 1_700_000_000_001,
            id: format!("{}:{}", source.lsn, source.tx_id),
        };

        // Serialize
        let json = serde_json::to_string(&event).expect("serialize");

        // Deserialize
        let deserialized: ChangeEvent = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_event_roundtrip_json_snapshot() {
        let source = SourceMetadata {
            db: "test_db".into(),
            schema: "public".into(),
            table: "users".into(),
            lsn: "0/0".into(),
            tx_id: "0".into(),
            ts_ms: 1_700_000_000_000,
            snapshot: Some(true),
        };

        let event = ChangeEvent {
            op: Operation::Read.as_str().to_string(),
            before: None,
            after: Some(serde_json::json!({"id": 1, "name": "Bob"})),
            source,
            ts_ms: 1_700_000_000_001,
            id: "snap:public.users:abc123".into(),
        };

        let json = serde_json::to_string(&event).expect("serialize");
        let deserialized: ChangeEvent = serde_json::from_str(&json).expect("deserialize");

        // snapshot field should survive round-trip when Some(true)
        assert_eq!(deserialized.source.snapshot, Some(true));
        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_source_metadata_skips_none_snapshot() {
        let source = SourceMetadata {
            db: "d".into(),
            schema: "s".into(),
            table: "t".into(),
            lsn: "0/1".into(),
            tx_id: "1".into(),
            ts_ms: 0,
            snapshot: None,
        };

        let json = serde_json::to_string(&source).expect("serialize");
        // When snapshot is None, the field should be absent from JSON
        assert!(!json.contains("snapshot"));
    }

    #[test]
    fn test_source_metadata_serializes_some_snapshot() {
        let source = SourceMetadata {
            db: "d".into(),
            schema: "s".into(),
            table: "t".into(),
            lsn: "0/1".into(),
            tx_id: "1".into(),
            ts_ms: 0,
            snapshot: Some(true),
        };

        let json = serde_json::to_string(&source).expect("serialize");
        assert!(json.contains("snapshot"));
    }
}
