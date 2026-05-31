//! Core event types — `ChangeEvent`, `SourceMetadata`, `Operation`, and `Lsn`.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::error::TapError;

// ---------------------------------------------------------------------------
// LSN newtype
// ---------------------------------------------------------------------------

/// A Postgres WAL Log Sequence Number.
///
/// Wraps the canonical hex-string representation (e.g. `0/1234567`).
/// Currently a lightweight string wrapper; ordering and full parsing
/// will be added when the replication module (P3) consumes this type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
pub struct Lsn(pub String);

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Lsn {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Lsn(s.to_string()))
    }
}

impl Lsn {
    /// Returns `true` when the LSN is the empty string.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

/// A single data-change event, modelled after the Debezium envelope format.
///
/// This struct represents one row-level change captured from a Postgres
/// replication stream.  The `op` field uses the [`Operation`] enum and is
/// serialised to/from single-character Debezium codes (`c`/`u`/`d`/`r`).
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
///     lsn: "0/1234567".parse().unwrap(),
///     tx_id: "12345".into(),
///     ts_ms: 1_700_000_000_000,
///     snapshot: None,
/// };
///
/// let event = ChangeEvent {
///     op: Operation::Create,
///     before: None,
///     after: Some(serde_json::json!({"id": 1, "name": "Alice"})),
///     source,
///     ts_ms: 1_700_000_000_001,
///     id: "0/1234567:12345".into(),
/// };
///
/// assert_eq!(event.op, Operation::Create);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChangeEvent {
    /// Operation type — serialised as Debezium single-character codes.
    #[serde(with = "operation_code")]
    pub op: Operation,
    /// Row state before the change (None for inserts).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<serde_json::Value>,
    /// Row state after the change (None for deletes).
    #[serde(skip_serializing_if = "Option::is_none")]
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct SourceMetadata {
    /// Source database name.
    pub db: String,
    /// Source schema name.
    pub schema: String,
    /// Source table name.
    pub table: String,
    /// Postgres WAL Log Sequence Number (e.g. `0/1234567`).
    #[serde(default, skip_serializing_if = "Lsn::is_empty")]
    pub lsn: Lsn,
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
}

impl FromStr for Operation {
    type Err = TapError;

    /// Parses a single-character operation code into an `Operation`.
    ///
    /// # Errors
    ///
    /// Returns [`TapError::Decode`] when the string is not one of
    /// `"c"`, `"u"`, `"d"`, or `"r"`.
    ///
    /// # Examples
    ///
    /// ```
    /// use tap_core::event::Operation;
    ///
    /// assert_eq!("c".parse::<Operation>().unwrap(), Operation::Create);
    /// assert!("x".parse::<Operation>().is_err());
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "c" => Ok(Operation::Create),
            "u" => Ok(Operation::Update),
            "d" => Ok(Operation::Delete),
            "r" => Ok(Operation::Read),
            other => Err(TapError::Decode(format!(
                "invalid operation code: {other:?} (expected c/u/d/r)"
            ))),
        }
    }
}

/// Custom serde helpers — serialise `Operation` as single-character codes.
mod operation_code {
    use super::Operation;
    use serde::de::{self, Deserializer};
    use serde::ser::Serializer;

    pub fn serialize<S: Serializer>(op: &Operation, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(op.as_str())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Operation, D::Error> {
        let code = <String as serde::Deserialize>::deserialize(d)?;
        code.parse::<Operation>().map_err(de::Error::custom)
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
        assert_eq!("c".parse::<Operation>().unwrap(), Operation::Create);
        assert_eq!("u".parse::<Operation>().unwrap(), Operation::Update);
        assert_eq!("d".parse::<Operation>().unwrap(), Operation::Delete);
        assert_eq!("r".parse::<Operation>().unwrap(), Operation::Read);
    }

    #[test]
    fn test_operation_from_str_invalid() {
        let err = "x".parse::<Operation>().unwrap_err();
        assert!(err.to_string().contains("invalid operation code"));
    }

    #[test]
    fn test_operation_from_str_empty() {
        let err = "".parse::<Operation>().unwrap_err();
        assert!(err.to_string().contains("invalid operation code"));
    }

    #[test]
    fn test_event_roundtrip_json() {
        let source = SourceMetadata {
            db: "test_db".into(),
            schema: "public".into(),
            table: "users".into(),
            lsn: Lsn("0/ABCDEF".into()),
            tx_id: "42".into(),
            ts_ms: 1_700_000_000_000,
            snapshot: None,
        };

        let event = ChangeEvent {
            op: Operation::Create,
            before: None,
            after: Some(serde_json::json!({"id": 1, "name": "Alice"})),
            source: source.clone(),
            ts_ms: 1_700_000_000_001,
            id: format!("{}:{}", source.lsn, source.tx_id),
        };

        let json = serde_json::to_string(&event).expect("serialize");
        let deserialized: ChangeEvent = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(event, deserialized);

        // Verify wire format uses single-char op codes
        assert!(json.contains(r#""op":"c""#));
    }

    #[test]
    fn test_event_roundtrip_json_snapshot() {
        let source = SourceMetadata {
            db: "test_db".into(),
            schema: "public".into(),
            table: "users".into(),
            lsn: Lsn("0/0".into()),
            tx_id: "0".into(),
            ts_ms: 1_700_000_000_000,
            snapshot: Some(true),
        };

        let event = ChangeEvent {
            op: Operation::Read,
            before: None,
            after: Some(serde_json::json!({"id": 1, "name": "Bob"})),
            source,
            ts_ms: 1_700_000_000_001,
            id: "snap:public.users:abc123".into(),
        };

        let json = serde_json::to_string(&event).expect("serialize");
        let deserialized: ChangeEvent = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(deserialized.source.snapshot, Some(true));
        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_source_metadata_skips_none_snapshot() {
        let source = SourceMetadata {
            db: "d".into(),
            schema: "s".into(),
            table: "t".into(),
            lsn: Lsn("0/1".into()),
            tx_id: "1".into(),
            ts_ms: 0,
            snapshot: None,
        };

        let json = serde_json::to_string(&source).expect("serialize");
        assert!(!json.contains("snapshot"));
    }

    #[test]
    fn test_source_metadata_serializes_some_snapshot() {
        let source = SourceMetadata {
            db: "d".into(),
            schema: "s".into(),
            table: "t".into(),
            lsn: Lsn("0/1".into()),
            tx_id: "1".into(),
            ts_ms: 0,
            snapshot: Some(true),
        };

        let json = serde_json::to_string(&source).expect("serialize");
        assert!(json.contains("snapshot"));
    }

    #[test]
    fn test_lsn_newtype() {
        let lsn: Lsn = "0/ABCDEF".parse().unwrap();
        assert_eq!(format!("{lsn}"), "0/ABCDEF");
        assert!(!lsn.is_empty());

        let empty = Lsn::default();
        assert!(empty.is_empty());

        // Lsn ordering (lexicographic for now)
        assert!(Lsn("0/1".into()) < Lsn("0/2".into()));
        assert_eq!(Lsn("0/1".into()), Lsn("0/1".into()));
    }

    #[test]
    fn test_change_event_json_op_roundtrip() {
        // Verify JSON deserialization of Debezium-style op codes
        let json = r#"{"op":"c","before":null,"after":null,"source":{"db":"d","schema":"s","table":"t","lsn":"0/1","tx_id":"1","ts_ms":0},"ts_ms":0,"id":"test"}"#;
        let event: ChangeEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.op, Operation::Create);
    }

    #[test]
    fn test_change_event_json_rejects_invalid_op() {
        let json = r#"{"op":"x","before":null,"after":null,"source":{"db":"d","schema":"s","table":"t","lsn":"0/1","tx_id":"1","ts_ms":0},"ts_ms":0,"id":"test"}"#;
        let result = serde_json::from_str::<ChangeEvent>(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_lsn_serde_roundtrip() {
        let lsn = Lsn("0/ABCDEF".into());
        let json = serde_json::to_string(&lsn).unwrap();
        assert_eq!(json, r#""0/ABCDEF""#);
        let back: Lsn = serde_json::from_str(&json).unwrap();
        assert_eq!(lsn, back);
    }
}
