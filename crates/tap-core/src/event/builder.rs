//! Builder for constructing `ChangeEvent` values with auto-generated
//! identifiers and timestamps.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use uuid::Uuid;

use super::envelope::{ChangeEvent, Operation, SourceMetadata};
use crate::error::TapError;

/// Monotonic timestamp counter.
///
/// Guarantees strictly increasing values even when the wall clock jumps
/// backwards (NTP corrections, leap seconds, VM pause).  Uses a CAS loop
/// for thread safety without blocking.
static MONO_TS: AtomicU64 = AtomicU64::new(0);

/// Returns a strictly monotonically increasing millisecond timestamp.
fn monotonic_ts_ms() -> u64 {
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    loop {
        let prev = MONO_TS.load(Ordering::Relaxed);
        let next = prev.max(wall) + 1; // always advance by at least 1
        if MONO_TS
            .compare_exchange_weak(prev, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return next;
        }
    }
}

#[cfg(test)]
impl ChangeEventBuilder {
    /// Sets a known expected `ts_ms`, bypassing monotonic generation.
    /// Only available in test builds.
    pub fn ts_ms(mut self, ts_ms: u64) -> Self {
        self.ts_ms = Some(ts_ms);
        self
    }
}

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
///     lsn: Some("0/1234567".into()),
///     binlog_file: None,
///     binlog_offset: None,
///     tx_id: "42".into(),
///     ts_ms: 1_700_000_000_000,
///     snapshot: None,
/// };
///
/// let event = ChangeEventBuilder::new()
///     .op(Operation::Create)
///     .after(Some(serde_json::json!({"id": 1, "name": "Alice"})))
///     .source(source)
///     .build()
///     .unwrap();
///
/// assert_eq!(event.op, Operation::Create);
/// assert_eq!(event.source.db, "mydb");
/// ```
#[derive(Debug, Clone)]
pub struct ChangeEventBuilder {
    op: Option<Operation>,
    before: Option<Option<serde_json::Value>>,
    after: Option<Option<serde_json::Value>>,
    source: Option<SourceMetadata>,
    ts_ms: Option<u64>,
}

impl ChangeEventBuilder {
    /// Creates a new builder with default values.
    pub fn new() -> Self {
        Self {
            op: None,
            before: None,
            after: None,
            source: None,
            ts_ms: None,
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
    /// * `ts_ms` — monotonic timestamp (see [`monotonic_ts_ms`])
    /// * `id` — from source metadata, format `{lsn}:{tx_id}` for streaming,
    ///   `snap:{schema}.{table}:{uuid}` for snapshot events
    ///
    /// # Errors
    ///
    /// Returns [`TapError::Config`] if `source` has not been set.
    pub fn build(self) -> Result<ChangeEvent, TapError> {
        let ts_ms = self.ts_ms.unwrap_or_else(monotonic_ts_ms);

        let source = self.source.ok_or_else(|| {
            TapError::Config("ChangeEventBuilder: source metadata is required".into())
        })?;
        let op = self.op.unwrap_or(Operation::Read);

        let id = Self::generate_id(&source);

        Ok(ChangeEvent {
            op,
            before: self.before.flatten(),
            after: self.after.flatten(),
            source,
            ts_ms,
            id,
        })
    }

    /// Generates a unique event identifier from source metadata.
    ///
    /// ## Determinism
    ///
    /// | Source | Fields used | Format |
    /// |--------|-------------|--------|
    /// | Postgres (streaming) | `lsn` + `tx_id` | `{lsn}:{tx_id}` |
    /// | MySQL (streaming) | `binlog_file` + `binlog_offset` + `tx_id` | `{file}:{offset}:{tx_id}` |
    /// | Snapshot (any) | random UUID suffix | `snap:{schema}.{table}:{uuid}` |
    /// | Fallback | random UUID | bare UUID |
    ///
    /// Deterministic IDs are essential for downstream deduplication — the same
    /// source position always produces the same event ID, enabling safe retry
    /// and exactly-once semantics.
    fn generate_id(source: &SourceMetadata) -> String {
        match source.snapshot {
            Some(true) => {
                // Snapshot IDs are unique-per-run; determinism not required.
                let short = Uuid::new_v4().to_string();
                format!("snap:{}.{}:{}", source.schema, source.table, short)
            }
            _ => {
                if let Some(ref lsn) = source.lsn {
                    // Postgres: deterministic from WAL position + transaction ID
                    format!("{}:{}", lsn, source.tx_id)
                } else if let (Some(ref file), Some(offset)) =
                    (source.binlog_file.clone(), source.binlog_offset)
                {
                    // MySQL: deterministic from binlog position + transaction ID
                    format!("{}:{}:{}", file, offset, source.tx_id)
                } else {
                    // No position metadata available (should not occur in practice).
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_all_ops() {
        let source = SourceMetadata {
            db: "db".into(),
            schema: "s".into(),
            table: "t".into(),
            lsn: Some("0/1".into()),
            binlog_file: None,
            binlog_offset: None,
            tx_id: "1".into(),
            ts_ms: 100,
            snapshot: None,
        };

        let ops = [
            Operation::Create,
            Operation::Update,
            Operation::Delete,
            Operation::Read,
        ];
        for op in &ops {
            let event = ChangeEventBuilder::new()
                .op(*op)
                .source(source.clone())
                .build()
                .unwrap();
            assert_eq!(event.op, *op, "op={:?} mismatch", op);
        }
    }

    #[test]
    fn test_builder_default_op_is_read() {
        let source = SourceMetadata {
            db: "db".into(),
            schema: "s".into(),
            table: "t".into(),
            lsn: None,
            binlog_file: None,
            binlog_offset: None,
            tx_id: String::new(),
            ts_ms: 0,
            snapshot: Some(true),
        };

        let event = ChangeEventBuilder::new().source(source).build().unwrap();
        assert_eq!(event.op, Operation::Read);
    }

    #[test]
    fn test_event_id_format_streaming() {
        let source = SourceMetadata {
            db: "db".into(),
            schema: "s".into(),
            table: "t".into(),
            lsn: Some("0/ABCDEF".into()),
            binlog_file: None,
            binlog_offset: None,
            tx_id: "123".into(),
            ts_ms: 0,
            snapshot: None,
        };

        let event = ChangeEventBuilder::new()
            .op(Operation::Create)
            .source(source)
            .build()
            .unwrap();

        assert_eq!(event.id, "0/ABCDEF:123");
    }

    #[test]
    fn test_event_id_format_snapshot() {
        let source = SourceMetadata {
            db: "db".into(),
            schema: "public".into(),
            table: "users".into(),
            lsn: Some("0/0".into()),
            binlog_file: None,
            binlog_offset: None,
            tx_id: "0".into(),
            ts_ms: 0,
            snapshot: Some(true),
        };

        let event = ChangeEventBuilder::new()
            .op(Operation::Read)
            .source(source)
            .build()
            .unwrap();

        assert!(event.id.starts_with("snap:public.users:"));
    }

    #[test]
    fn test_event_id_fallback_uuid() {
        let source = SourceMetadata {
            db: "db".into(),
            schema: "s".into(),
            table: "t".into(),
            lsn: None,
            binlog_file: None,
            binlog_offset: None,
            tx_id: String::new(),
            ts_ms: 0,
            snapshot: None,
        };

        let event = ChangeEventBuilder::new()
            .op(Operation::Read)
            .source(source)
            .build()
            .unwrap();

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
            .build()
            .unwrap();

        assert_eq!(event.before, Some(before_val));
        assert_eq!(event.after, Some(after_val));
    }

    #[test]
    fn test_builder_ts_ms_monotonic() {
        let source = SourceMetadata::default();

        let e1 = ChangeEventBuilder::new()
            .op(Operation::Create)
            .source(source.clone())
            .build()
            .unwrap();
        let e2 = ChangeEventBuilder::new()
            .op(Operation::Create)
            .source(source)
            .build()
            .unwrap();

        // ts_ms must be strictly increasing
        assert!(
            e2.ts_ms > e1.ts_ms,
            "ts_ms not monotonic: {} then {}",
            e1.ts_ms,
            e2.ts_ms
        );
    }

    #[test]
    fn test_builder_missing_source_returns_error() {
        let result = ChangeEventBuilder::new().op(Operation::Create).build();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("source metadata is required")
        );
    }

    #[test]
    fn test_builder_ts_ms_overridden_in_test() {
        let source = SourceMetadata::default();
        let event = ChangeEventBuilder::new()
            .op(Operation::Create)
            .source(source)
            .ts_ms(42)
            .build()
            .unwrap();
        assert_eq!(event.ts_ms, 42);
    }
}
