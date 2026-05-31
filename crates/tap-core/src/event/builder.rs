//! Builder for constructing `ChangeEvent` values with auto-generated
//! identifiers and timestamps.

use std::time::{SystemTime, UNIX_EPOCH};

use uuid::Uuid;

use super::envelope::{ChangeEvent, Operation, SourceMetadata};

/// Builder for [`ChangeEvent`].
///
/// Handles the common boilerplate of generating event IDs and timestamps.
///
/// # Examples
///
/// ```
/// use tap_core::event::{ChangeEventBuilder, Operation, SourceMetadata};
///
/// let source = SourceMetadata {
///     db: "mydb".into(),
///     schema: "public".into(),
///     table: "users".into(),
///     lsn: "0/1234567".into(),
///     tx_id: "42".into(),
///     ts_ms: 1_700_000_000_000,
///     snapshot: None,
/// };
///
/// let event = ChangeEventBuilder::new()
///     .op(Operation::Create)
///     .after(Some(serde_json::json!({"id": 1, "name": "Alice"})))
///     .source(source)
///     .build();
///
/// assert_eq!(event.op, "c");
/// assert_eq!(event.source.db, "mydb");
/// ```
#[derive(Debug, Clone)]
pub struct ChangeEventBuilder {
    op: Option<Operation>,
    before: Option<Option<serde_json::Value>>,
    after: Option<Option<serde_json::Value>>,
    source: Option<SourceMetadata>,
}

impl ChangeEventBuilder {
    /// Creates a new builder with default values.
    pub fn new() -> Self {
        Self {
            op: None,
            before: None,
            after: None,
            source: None,
        }
    }

    /// Sets the operation type.
    pub fn op(mut self, op: Operation) -> Self {
        self.op = Some(op);
        self
    }

    /// Sets the row state before the change.
    pub fn before(mut self, before: Option<serde_json::Value>) -> Self {
        self.before = Some(before);
        self
    }

    /// Sets the row state after the change.
    pub fn after(mut self, after: Option<serde_json::Value>) -> Self {
        self.after = Some(after);
        self
    }

    /// Sets the source metadata.
    pub fn source(mut self, source: SourceMetadata) -> Self {
        self.source = Some(source);
        self
    }

    /// Consumes the builder and produces a [`ChangeEvent`].
    ///
    /// Auto-generates:
    /// * `ts_ms` — current system time in milliseconds since UNIX epoch
    /// * `id` — from source metadata, format `{lsn}:{tx_id}` for streaming,
    ///   `snap:{schema}.{table}:{uuid}` for snapshot events
    ///
    /// Missing optional fields (`op`, `source`, `before`, `after`) are
    /// filled with sensible defaults (op defaults to `Read`, source
    /// defaults to an empty record).
    pub fn build(self) -> ChangeEvent {
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let source = self.source.unwrap_or_default();
        let op = self
            .op
            .map(|o| o.as_str().to_string())
            .unwrap_or_else(|| Operation::Read.as_str().to_string());

        let id = Self::generate_id(&source);

        ChangeEvent {
            op,
            before: self.before.flatten(),
            after: self.after.flatten(),
            source,
            ts_ms,
            id,
        }
    }

    /// Generates a unique event identifier from source metadata.
    fn generate_id(source: &SourceMetadata) -> String {
        match source.snapshot {
            Some(true) => {
                // Snapshot events use a predictable prefix with a short UUID
                let short = Uuid::new_v4().to_string();
                format!("snap:{}.{}:{}", source.schema, source.table, short)
            }
            _ => {
                // Streaming events use `{lsn}:{tx_id}` when available
                if !source.lsn.is_empty() {
                    format!("{}:{}", source.lsn, source.tx_id)
                } else {
                    Uuid::new_v4().to_string()
                }
            }
        }
    }
}

impl Default for ChangeEventBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for SourceMetadata {
    fn default() -> Self {
        Self {
            db: String::new(),
            schema: String::new(),
            table: String::new(),
            lsn: String::new(),
            tx_id: String::new(),
            ts_ms: 0,
            snapshot: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_all_ops() {
        let source = SourceMetadata {
            db: "db".into(),
            schema: "s".into(),
            table: "t".into(),
            lsn: "0/1".into(),
            tx_id: "1".into(),
            ts_ms: 100,
            snapshot: None,
        };

        let ops = [
            (Operation::Create, "c"),
            (Operation::Update, "u"),
            (Operation::Delete, "d"),
            (Operation::Read, "r"),
        ];

        for (op, expected) in &ops {
            let event = ChangeEventBuilder::new()
                .op(*op)
                .source(source.clone())
                .build();
            assert_eq!(event.op, *expected, "op={:?} should produce '{}'", op, expected);
        }
    }

    #[test]
    fn test_builder_default_op_is_read() {
        let source = SourceMetadata {
            db: "db".into(),
            schema: "s".into(),
            table: "t".into(),
            lsn: String::new(),
            tx_id: String::new(),
            ts_ms: 0,
            snapshot: Some(true),
        };

        let event = ChangeEventBuilder::new().source(source).build();
        assert_eq!(event.op, "r");
    }

    #[test]
    fn test_event_id_format_streaming() {
        let source = SourceMetadata {
            db: "db".into(),
            schema: "s".into(),
            table: "t".into(),
            lsn: "0/ABCDEF".into(),
            tx_id: "123".into(),
            ts_ms: 0,
            snapshot: None,
        };

        let event = ChangeEventBuilder::new()
            .op(Operation::Create)
            .source(source)
            .build();

        assert_eq!(event.id, "0/ABCDEF:123");
    }

    #[test]
    fn test_event_id_format_snapshot() {
        let source = SourceMetadata {
            db: "db".into(),
            schema: "public".into(),
            table: "users".into(),
            lsn: "0/0".into(),
            tx_id: "0".into(),
            ts_ms: 0,
            snapshot: Some(true),
        };

        let event = ChangeEventBuilder::new()
            .op(Operation::Read)
            .source(source)
            .build();

        assert!(event.id.starts_with("snap:public.users:"));
    }

    #[test]
    fn test_event_id_fallback_uuid() {
        let source = SourceMetadata {
            db: "db".into(),
            schema: "s".into(),
            table: "t".into(),
            lsn: String::new(),
            tx_id: String::new(),
            ts_ms: 0,
            snapshot: None,
        };

        let event = ChangeEventBuilder::new()
            .op(Operation::Read)
            .source(source)
            .build();

        // When LSN is empty, fall back to a UUID
        assert!(!event.id.is_empty());
        assert!(!event.id.contains(':'));
    }

    #[test]
    fn test_builder_before_after() {
        let source = SourceMetadata::default();
        let before_val = serde_json::json!({"id": 0});
        let after_val = serde_json::json!({"id": 1, "name": "test"});

        let event = ChangeEventBuilder::new()
            .op(Operation::Update)
            .before(Some(before_val.clone()))
            .after(Some(after_val.clone()))
            .source(source)
            .build();

        assert_eq!(event.before, Some(before_val));
        assert_eq!(event.after, Some(after_val));
    }

    #[test]
    fn test_builder_ts_ms_generated() {
        let source = SourceMetadata::default();
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let event = ChangeEventBuilder::new()
            .op(Operation::Create)
            .source(source)
            .build();

        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        assert!(event.ts_ms >= before);
        assert!(event.ts_ms <= after);
    }
}
