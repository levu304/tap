//! WAL decoding for PostgreSQL logical replication.
//!
//! Provides the [`WalDecoder`] trait and two implementations:
//!
//! * [`PgoutputDecoder`] — hand-written binary parser for the pgoutput
//!   protocol (the default for Postgres 10+).
//! * [`Wal2JsonDecoder`] — parser for the wal2json JSON format.
//!
//! # pgoutput Protocol
//!
//! The pgoutput logical replication plugin emits a binary protocol
//! documented in the PostgreSQL source code
//! (`src/backend/replication/pgoutput/pgoutput.c`).  Each message
//! starts with a single-byte type tag:
//!
//! | Byte | Type       | Payload                                  |
//! |------|------------|------------------------------------------|
//! | `B`  | Begin      | LSN, commit_time_us, xid                 |
//! | `I`  | Insert     | relation_id, tuple_type, TupleData       |
//! | `U`  | Update     | relation_id, old_tuple_type?, TupleData* |
//! | `D`  | Delete     | relation_id, old_tuple_type, TupleData   |
//! | `C`  | Commit     | Int8 flags, commit_lsn, end_lsn, ts      |
//! | `R`  | Relation   | relation_id, schema, table, columns...   |
//! | `Y`  | Type       | (skipped)                                |
//! | `O`  | Origin     | (skipped)                                |
//! | `T`  | Truncate   | (skipped)                                |
//!
//! All multi-byte integers are **network byte order** (big-endian).
//! Strings are null-terminated C strings.
//!
//! ## TupleData format
//!
//! ```text
//! Int16: number of columns
//! For each column:
//!   Byte1:  'n' = NULL, 'u' = unchanged toast, 't' = text value
//!   if 't':
//!     Int32:   length
//!     Byte[n]: data
//! ```
//!
//! # wal2json format
//!
//! One JSON blob per transaction with a `"change"` array:
//!
//! ```json
//! {"xid":123,"timestamp":"...","change":[{"kind":"insert","schema":"public",
//!   "table":"users",...}]}
//! ```

use std::collections::HashMap;

use serde_json::Value as JsonValue;
use tracing::{debug, warn};

use crate::error::TapError;

// ---------------------------------------------------------------------------
// Re-export the event Lsn with an alias to avoid shadowing postgres::Lsn
// ---------------------------------------------------------------------------

use crate::event::Lsn as EventLsn;
use crate::event::{ChangeEvent, Operation, SourceMetadata};
use crate::postgres::Lsn as PgLsn;

// ---------------------------------------------------------------------------
// Schema cache types
// ---------------------------------------------------------------------------

/// Schema cache entry populated by pgoutput Relation messages.
///
/// Maps a server-side relation OID to the logical schema, table name, and
/// column descriptors so that subsequent Insert / Update / Delete messages
/// can be decoded against the correct schema.
#[derive(Debug, Clone)]
pub struct RelationSchema {
    /// Server-side relation OID.
    pub relation_id: u32,
    /// Namespace (schema) name, e.g. `"public"`.
    pub schema: String,
    /// Table name, e.g. `"users"`.
    pub table: String,
    /// Column descriptors in ordinal order.
    pub columns: Vec<ColumnInfo>,
}

/// Descriptor for a single column in a [`RelationSchema`].
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Postgres type OID (e.g. `23` for `int4`, `25` for `text`).
    pub typ: u32,
    /// Type modifier (e.g. varchar precision), or `-1` if none.
    pub modifier: i32,
}

// ---------------------------------------------------------------------------
// DecodeResult
// ---------------------------------------------------------------------------

/// Result from a single [`WalDecoder::decode()`] call.
#[derive(Debug)]
pub struct DecodeResult {
    /// Decoded change events (non-empty when a transaction was committed).
    pub events: Vec<ChangeEvent>,
    /// Commit LSN extracted from the decoded data, if available.
    ///
    /// * `pgoutput` — the commit LSN from the `Commit` message.
    /// * `wal2json` — `None` (wal2json does not include LSNs in its JSON).
    ///
    /// When `None`, the caller should fall back to the replication stream's
    /// current position (e.g. [`ReplicationStream::current_lsn`]).
    pub commit_lsn: Option<PgLsn>,
}

// ---------------------------------------------------------------------------
// WalDecoder trait
// ---------------------------------------------------------------------------

/// Trait for decoding WAL messages into [`ChangeEvent`]s.
///
/// Implementations are expected to maintain internal state (schema cache,
/// transaction boundaries) across calls to [`decode()`](Self::decode).
///
/// # Thread safety
///
/// Implementations must be `Send + Sync` so that a decoder can be shared
/// across tasks if needed.  The trait takes `&mut self` to allow stateful
/// streaming.
pub trait WalDecoder: Send + Sync {
    /// Decode a single WAL message (or batch) into decoded events and an
    /// optional commit LSN.
    ///
    /// * For **pgoutput** the buffer may contain multiple concatenated
    ///   protocol messages.  Begin / Relation / Insert / Update / Delete
    ///   messages are accumulated; Commit messages finalise pending events.
    ///   The returned events vector is non-empty only when a Commit is
    ///   processed, and `commit_lsn` is set to the Commit's LSN.
    /// * For **wal2json** each call receives one complete JSON transaction
    ///   blob and returns all change rows as events.  `commit_lsn` is `None`
    ///   because wal2json does not carry per-event LSNs.
    ///
    /// # Errors
    ///
    /// Returns [`TapError::Decode`] for malformed or corrupt input.
    fn decode(&mut self, message: &[u8]) -> Result<DecodeResult, TapError>;

    /// Human-readable decoder name (e.g. `"pgoutput"`, `"wal2json"`).
    fn name(&self) -> &'static str;

    /// Flush any pending events that have not yet been committed.
    ///
    /// May be called before dropping a decoder to recover in-flight events.
    fn flush(&mut self) -> Vec<ChangeEvent> {
        Vec::new()
    }
}

// ---------------------------------------------------------------------------
// PgoutputDecoder
// ---------------------------------------------------------------------------

/// Decoder for the PostgreSQL pgoutput logical replication protocol.
///
/// Maintains a schema cache populated by `Relation` messages and tracks
/// the current transaction's events across `Begin` / DML / `Commit`
/// boundaries.  Events are only emitted when a `Commit` message arrives.
///
/// # Binary format
///
/// See the [module-level documentation](self) for the protocol structure,
/// or refer to the PostgreSQL source
/// (`src/backend/replication/pgoutput/pgoutput.c`).
pub struct PgoutputDecoder {
    /// Schema cache — relation OID → schema descriptor.
    schema_cache: HashMap<u32, RelationSchema>,

    /// Source database name.
    db_name: String,

    // ── Transaction state ────────────────────────────────────────────────
    /// LSN of the current transaction (from `Begin`).
    current_lsn: Option<PgLsn>,
    /// Transaction ID of the current transaction (from `Begin`).
    current_tx_id: Option<String>,
    /// Commit timestamp (Unix epoch ms) of the current transaction.
    current_ts_ms: Option<u64>,
    /// Events accumulated for the current transaction.
    pending_events: Vec<PendingEvent>,
    /// LSN from the last committed transaction, returned via [`DecodeResult`].
    last_commit_lsn: Option<PgLsn>,
}

/// An event that has been partially decoded and is waiting for a Commit
/// message to finalise its LSN and identity.
#[derive(Debug, Clone)]
struct PendingEvent {
    op: Operation,
    before: Option<JsonValue>,
    after: Option<JsonValue>,
    schema: String,
    table: String,
}

impl PgoutputDecoder {
    /// Maximum number of pending events allowed per transaction.
    const MAX_EVENTS_PER_TXN: usize = 10_000;

    /// Create a new decoder for the given database with an empty schema cache.
    pub fn new(db_name: impl Into<String>) -> Self {
        Self {
            schema_cache: HashMap::new(),
            db_name: db_name.into(),
            current_lsn: None,
            current_tx_id: None,
            current_ts_ms: None,
            pending_events: Vec::new(),
            last_commit_lsn: None,
        }
    }

    // ── Top-level dispatch ───────────────────────────────────────────

    /// Parse zero or more pgoutput messages from `buf` starting at `offset`.
    /// Returns accumulated events (only non-empty after a Commit).
    fn decode_messages(
        &mut self,
        buf: &[u8],
        offset: &mut usize,
    ) -> Result<Vec<ChangeEvent>, TapError> {
        let mut events = Vec::new();

        while *offset < buf.len() {
            let msg_type = buf[*offset];
            *offset += 1;

            match msg_type {
                b'B' => self.decode_begin(buf, offset)?,
                b'I' => self.decode_insert(buf, offset)?,
                b'U' => self.decode_update(buf, offset)?,
                b'D' => self.decode_delete(buf, offset)?,
                b'C' => {
                    let mut commit_events = self.decode_commit(buf, offset)?;
                    events.append(&mut commit_events);
                }
                b'R' => self.decode_relation(buf, offset)?,
                // Known ignorable types — parse enough to advance offset,
                // then discard the data.
                b'Y' => self.skip_type(buf, offset)?,
                b'O' => self.skip_origin(buf, offset)?,
                b'T' => self.skip_truncate(buf, offset)?,
                other => {
                    debug!(
                        "skipping unknown pgoutput message type byte: 0x{:02x}",
                        other
                    );
                    // We cannot safely skip an unknown message structure,
                    // so return what we have rather than enter a spin loop.
                    break;
                }
            }
        }

        Ok(events)
    }

    // ── Begin ('b') ─────────────────────────────────────────────────

    /// Format: `'b' | Int64 lsn | Int64 commit_time_us | Int32 xid`
    fn decode_begin(&mut self, buf: &[u8], offset: &mut usize) -> Result<(), TapError> {
        let raw_lsn = read_i64(buf, offset)? as u64;
        let commit_time_us = read_i64(buf, offset)?;
        let xid = read_i32(buf, offset)? as u64;

        self.current_lsn = Some(PgLsn::from_u64(raw_lsn));
        self.current_tx_id = Some(xid.to_string());
        self.current_ts_ms = Some(pg_timestamp_to_unix_ms(commit_time_us));
        if !self.pending_events.is_empty() {
            warn!(
                "discarding {} uncommitted events on new Begin (possible data loss)",
                self.pending_events.len()
            );
            self.pending_events.clear();
        }

        Ok(())
    }

    // ── Insert ('i') ────────────────────────────────────────────────

    /// Format: `'i' | Int32 relation_id | Byte1 'N' | TupleData`
    fn decode_insert(&mut self, buf: &[u8], offset: &mut usize) -> Result<(), TapError> {
        let relation_id = read_i32(buf, offset)? as u32;
        let tuple_type = read_u8(buf, offset)?;

        match tuple_type {
            b'N' | b'K' => {} // New tuple or key tuple — both decoded as "after"
            other => {
                return Err(TapError::Decode(format!(
                    "unexpected tuple type in Insert: 0x{:02x} (expected 'N' or 'K')",
                    other
                )));
            }
        }

        let schema = self.lookup_schema(relation_id)?;
        let values = self.parse_tuple_data(buf, offset, &schema.columns)?;

        if self.pending_events.len() >= Self::MAX_EVENTS_PER_TXN {
            return Err(TapError::Decode(format!(
                "transaction exceeds maximum of {} events",
                Self::MAX_EVENTS_PER_TXN
            )));
        }
        self.pending_events.push(PendingEvent {
            op: Operation::Create,
            before: None,
            after: Some(values),
            schema: schema.schema.clone(),
            table: schema.table.clone(),
        });

        Ok(())
    }

    // ── Update ('u') ────────────────────────────────────────────────

    /// Format: `'u' | Int32 relation_id | Byte1 tuple_type [TupleData]*
    ///
    /// If `tuple_type` is `'K'` (old key) or `'O'` (old tuple) the old
    /// TupleData is present.  `'N'` means new-tuple-only.  A new tuple
    /// always follows.
    fn decode_update(&mut self, buf: &[u8], offset: &mut usize) -> Result<(), TapError> {
        let relation_id = read_i32(buf, offset)? as u32;
        let tuple_type = read_u8(buf, offset)?;
        let schema = self.lookup_schema(relation_id)?;

        let (old_values, has_new) = match tuple_type {
            b'K' | b'O' => {
                let old = self.parse_tuple_data(buf, offset, &schema.columns)?;
                (Some(old), true)
            }
            b'N' => (None, true),
            other => {
                return Err(TapError::Decode(format!(
                    "unexpected tuple type in Update: 0x{:02x} (expected 'K', 'O', or 'N')",
                    other
                )));
            }
        };

        let new_values = if has_new {
            Some(self.parse_tuple_data(buf, offset, &schema.columns)?)
        } else {
            None
        };

        if self.pending_events.len() >= Self::MAX_EVENTS_PER_TXN {
            return Err(TapError::Decode(format!(
                "transaction exceeds maximum of {} events",
                Self::MAX_EVENTS_PER_TXN
            )));
        }
        self.pending_events.push(PendingEvent {
            op: Operation::Update,
            before: old_values,
            after: new_values,
            schema: schema.schema.clone(),
            table: schema.table.clone(),
        });

        Ok(())
    }

    // ── Delete ('d') ────────────────────────────────────────────────

    /// Format: `'d' | Int32 relation_id | Byte1 'K'/'O' | TupleData`
    fn decode_delete(&mut self, buf: &[u8], offset: &mut usize) -> Result<(), TapError> {
        let relation_id = read_i32(buf, offset)? as u32;
        let tuple_type = read_u8(buf, offset)?;
        let schema = self.lookup_schema(relation_id)?;

        match tuple_type {
            b'K' | b'O' => {} // old key or old tuple — expected
            other => {
                return Err(TapError::Decode(format!(
                    "unexpected tuple type in Delete: 0x{:02x} (expected 'K' or 'O')",
                    other
                )));
            }
        }

        let old_values = self.parse_tuple_data(buf, offset, &schema.columns)?;

        if self.pending_events.len() >= Self::MAX_EVENTS_PER_TXN {
            return Err(TapError::Decode(format!(
                "transaction exceeds maximum of {} events",
                Self::MAX_EVENTS_PER_TXN
            )));
        }
        self.pending_events.push(PendingEvent {
            op: Operation::Delete,
            before: Some(old_values),
            after: None,
            schema: schema.schema.clone(),
            table: schema.table.clone(),
        });

        Ok(())
    }

    // ── Commit ('c') ────────────────────────────────────────────────

    /// Format: `'c' | Int64 flags | Int64 commit_lsn | Int64 end_lsn | Int64 ts_us`
    fn decode_commit(
        &mut self,
        buf: &[u8],
        offset: &mut usize,
    ) -> Result<Vec<ChangeEvent>, TapError> {
        let _flags = read_i8(buf, offset)?;
        let commit_lsn = PgLsn::from_u64(read_i64(buf, offset)? as u64);
        self.last_commit_lsn = Some(commit_lsn);
        let _end_lsn = read_i64(buf, offset)?;
        let ts_us = read_i64(buf, offset)?;

        let ts_ms = pg_timestamp_to_unix_ms(ts_us);
        let lsn_display = commit_lsn.to_string();
        let event_lsn = EventLsn(lsn_display);

        let tx_id = self
            .current_tx_id
            .clone()
            .ok_or_else(|| TapError::Decode("Commit received without prior Begin".into()))?;

        let mut events = Vec::with_capacity(self.pending_events.len());

        for (idx, pending) in self.pending_events.drain(..).enumerate() {
            let source = SourceMetadata {
                db: self.db_name.clone(),
                schema: pending.schema,
                table: pending.table,
                lsn: event_lsn.clone(),
                tx_id: tx_id.clone(),
                ts_ms,
                snapshot: None,
            };

            let id = format!("{}:{}:{}", event_lsn, tx_id, idx);

            events.push(ChangeEvent {
                op: pending.op,
                before: pending.before,
                after: pending.after,
                source,
                ts_ms,
                id,
            });
        }

        // Reset transaction state
        self.current_lsn = None;
        self.current_tx_id = None;
        self.current_ts_ms = None;

        Ok(events)
    }

    // ── Relation ('r') — populates schema cache ─────────────────────

    /// Format:
    ///   `'r' | Int32 relation_id | String nsp | String relname |
    ///    Int8 replica_identity | Int16 ncols | ColumnInfo[ncols]`
    ///
    /// Each ColumnInfo:
    ///   `Int8 flags | String name | Int32 type_oid | Int32 type_modifier`
    fn decode_relation(&mut self, buf: &[u8], offset: &mut usize) -> Result<(), TapError> {
        let relation_id = read_i32(buf, offset)? as u32;
        let schema = read_cstring(buf, offset)?;
        let table = read_cstring(buf, offset)?;
        let _replica_identity = read_i8(buf, offset)?;
        let ncols_raw = read_i16(buf, offset)?;
        if ncols_raw < 0 {
            return Err(TapError::Decode(format!(
                "negative column count in Relation: {ncols_raw}"
            )));
        }
        let ncols = ncols_raw as usize;

        let mut columns = Vec::with_capacity(ncols);
        for _ in 0..ncols {
            let _flags = read_i8(buf, offset)?; // 1 = part of key
            let name = read_cstring(buf, offset)?;
            let typ = read_i32(buf, offset)? as u32;
            let modifier = read_i32(buf, offset)?;
            columns.push(ColumnInfo {
                name,
                typ,
                modifier,
            });
        }

        self.schema_cache.insert(
            relation_id,
            RelationSchema {
                relation_id,
                schema,
                table,
                columns,
            },
        );

        Ok(())
    }

    // ── Type ('y') ──────────────────────────────────────────────────

    /// Format: `'y' | Int32 oid | String nsp | String name` — skip.
    fn skip_type(&mut self, buf: &[u8], offset: &mut usize) -> Result<(), TapError> {
        let _oid = read_i32(buf, offset)?; // type OID
        let _nsp = read_cstring(buf, offset)?; // namespace
        let _name = read_cstring(buf, offset)?; // type name
        debug!("skipped Type message (oid={})", _oid);
        Ok(())
    }

    // ── Origin ('o') ────────────────────────────────────────────────

    /// Format: `'o' | String name | Int64 lsn` — skip.
    fn skip_origin(&mut self, buf: &[u8], offset: &mut usize) -> Result<(), TapError> {
        let _name = read_cstring(buf, offset)?;
        let _lsn = read_i64(buf, offset)?;
        debug!("skipped Origin message (name={})", _name);
        Ok(())
    }

    // ── Truncate ('t') ──────────────────────────────────────────────

    /// Format: `'t' | Int32 nrels | Int32 relids[nrels] | Byte1 options` — skip.
    fn skip_truncate(&mut self, buf: &[u8], offset: &mut usize) -> Result<(), TapError> {
        let nrels_raw = read_i32(buf, offset)?;
        if nrels_raw < 0 {
            return Err(TapError::Decode(format!(
                "negative relation count in Truncate: {nrels_raw}"
            )));
        }
        let nrels = nrels_raw as usize;
        for _ in 0..nrels {
            let _relid = read_i32(buf, offset)?;
        }
        let _options = read_u8(buf, offset)?;
        debug!("skipped Truncate message ({} relations)", nrels);
        Ok(())
    }

    // ── TupleData parser ────────────────────────────────────────────

    /// Parse a TupleData block and produce a JSON object mapping column names
    /// to values.
    ///
    /// Format:
    /// ```text
    /// Int16: ncols
    /// For each col:
    ///   Byte1:  'n' = NULL, 'u' = unchanged toast, 't' = text value
    ///   if 't':
    ///     Int32:   length
    ///     Byte[n]: data
    /// ```
    fn parse_tuple_data(
        &self,
        buf: &[u8],
        offset: &mut usize,
        columns: &[ColumnInfo],
    ) -> Result<JsonValue, TapError> {
        let ncols_raw = read_i16(buf, offset)?;
        if ncols_raw < 0 {
            return Err(TapError::Decode(format!(
                "negative column count in TupleData: {ncols_raw}"
            )));
        }
        let ncols = ncols_raw as usize;

        // pgoutput may send fewer columns than the Relation schema describes
        // (e.g. for TOAST'd columns) — we only iterate over what's actually
        // sent and map by ordinal position.
        let mut map = serde_json::Map::with_capacity(ncols.min(columns.len()));

        for i in 0..ncols {
            let col_name = columns
                .get(i)
                .map(|c| c.name.clone())
                .unwrap_or_else(|| format!("__col_{i}"));
            let col_type = read_u8(buf, offset)?;

            match col_type {
                b'n' | b'u' => {
                    // NULL or unchanged TOAST — both become JSON null
                    map.insert(col_name.to_string(), JsonValue::Null);
                }
                b't' => {
                    let len = read_i32(buf, offset)?;
                    if len < 0 {
                        // Negative length indicates NULL in pgoutput
                        map.insert(col_name.to_string(), JsonValue::Null);
                        continue;
                    }
                    let data = read_bytes(buf, offset, len as usize)?;
                    let value = raw_bytes_to_json(data);
                    map.insert(col_name.to_string(), value);
                }
                other => {
                    return Err(TapError::Decode(format!(
                        "unknown column type byte in TupleData: 0x{:02x}",
                        other
                    )));
                }
            }
        }

        Ok(JsonValue::Object(map))
    }

    /// Look up a schema by relation OID, returning an error when unknown.
    fn lookup_schema(&self, relation_id: u32) -> Result<&RelationSchema, TapError> {
        self.schema_cache.get(&relation_id).ok_or_else(|| {
            TapError::Decode(format!(
                "no schema cache entry for relation_id={}; \
                 Relation message must precede DML",
                relation_id
            ))
        })
    }
}

impl Default for PgoutputDecoder {
    fn default() -> Self {
        Self::new("")
    }
}

impl WalDecoder for PgoutputDecoder {
    fn decode(&mut self, message: &[u8]) -> Result<DecodeResult, TapError> {
        let mut offset = 0usize;
        let events = self.decode_messages(message, &mut offset)?;
        let commit_lsn = self.last_commit_lsn.take();
        Ok(DecodeResult { events, commit_lsn })
    }

    fn name(&self) -> &'static str {
        "pgoutput"
    }

    fn flush(&mut self) -> Vec<ChangeEvent> {
        let events: Vec<ChangeEvent> = self
            .pending_events
            .drain(..)
            .map(|pending| {
                let source = SourceMetadata {
                    db: self.db_name.clone(),
                    schema: pending.schema,
                    table: pending.table,
                    lsn: EventLsn(self.current_lsn.map(|l| l.to_string()).unwrap_or_default()),
                    tx_id: self.current_tx_id.clone().unwrap_or_default(),
                    ts_ms: self.current_ts_ms.unwrap_or(0),
                    snapshot: None,
                };
                let ts_ms = source.ts_ms;
                let id = format!("{}:{}:flush", source.lsn, source.tx_id);
                ChangeEvent {
                    op: pending.op,
                    before: pending.before,
                    after: pending.after,
                    source,
                    ts_ms,
                    id,
                }
            })
            .collect();
        self.current_lsn = None;
        self.current_tx_id = None;
        self.current_ts_ms = None;
        events
    }
}

// ---------------------------------------------------------------------------
// Wal2JsonDecoder
// ---------------------------------------------------------------------------

/// Decoder for the wal2json logical replication plugin format.
///
/// Each call to [`decode()`](WalDecoder::decode) receives a complete JSON
/// blob representing one transaction.  The JSON is parsed and each entry in
/// the `"change"` array becomes a [`ChangeEvent`].
///
/// # Limitations
///
/// * wal2json only provides `after` values for inserts and updates.
/// * `before` values are only available as old-key tuples for updates
///   and deletes (not full row images).
/// * There is no schema cache — schema and table names are provided
///   inline in each change entry.
pub struct Wal2JsonDecoder {
    /// Source database name.
    db_name: String,
}

impl Wal2JsonDecoder {
    /// Create a new decoder for the given database.
    pub fn new(db_name: impl Into<String>) -> Self {
        Self {
            db_name: db_name.into(),
        }
    }
}

impl WalDecoder for Wal2JsonDecoder {
    fn decode(&mut self, message: &[u8]) -> Result<DecodeResult, TapError> {
        let text = std::str::from_utf8(message)
            .map_err(|e| TapError::Decode(format!("wal2json input is not valid UTF-8: {e}")))?;

        let root: JsonValue = serde_json::from_str(text)
            .map_err(|e| TapError::Decode(format!("wal2json parse error: {e}")))?;

        let obj = root
            .as_object()
            .ok_or_else(|| TapError::Decode("wal2json root is not a JSON object".into()))?;

        let xid = obj
            .get("xid")
            .and_then(|v| v.as_u64())
            .map(|v| v.to_string())
            .unwrap_or_default();

        let ts_ms = parse_wal2json_timestamp(obj.get("timestamp"))?;

        let changes = obj
            .get("change")
            .and_then(|v| v.as_array())
            .ok_or_else(|| TapError::Decode("wal2json missing 'change' array".into()))?;

        let mut events = Vec::with_capacity(changes.len());

        for change in changes {
            let change_obj = change
                .as_object()
                .ok_or_else(|| TapError::Decode("wal2json change entry is not an object".into()))?;

            let kind = change_obj
                .get("kind")
                .and_then(|v| v.as_str())
                .ok_or_else(|| TapError::Decode("wal2json change entry missing 'kind'".into()))?;

            let schema = change_obj
                .get("schema")
                .and_then(|v| v.as_str())
                .unwrap_or("public")
                .to_string();

            let table = change_obj
                .get("table")
                .and_then(|v| v.as_str())
                .ok_or_else(|| TapError::Decode("wal2json change entry missing 'table'".into()))?;

            let (op, before, after) = match kind {
                "insert" => {
                    let after = build_json_from_columns(
                        change_obj.get("columnnames"),
                        change_obj.get("columntypes"),
                        change_obj.get("columnvalues"),
                    )?;
                    (Operation::Create, None, after)
                }
                "update" => {
                    let after = build_json_from_columns(
                        change_obj.get("columnnames"),
                        change_obj.get("columntypes"),
                        change_obj.get("columnvalues"),
                    )?;
                    let before = build_json_from_oldkeys(change_obj.get("oldkeys"))?;
                    (Operation::Update, before, after)
                }
                "delete" => {
                    let before = build_json_from_oldkeys(change_obj.get("oldkeys"))?;
                    (Operation::Delete, before, None)
                }
                other => {
                    debug!("skipping wal2json change kind '{}'", other);
                    continue;
                }
            };

            // Build the LSN from available data — wal2json doesn't carry
            // per-event LSNs, so we use an empty string.
            let event_lsn = EventLsn(String::new());
            let source = SourceMetadata {
                db: self.db_name.clone(),
                schema,
                table: table.to_string(),
                lsn: event_lsn.clone(),
                tx_id: xid.clone(),
                ts_ms,
                snapshot: None,
            };
            let id = if xid.is_empty() {
                format!("{}:{}", event_lsn, uuid::Uuid::new_v4())
            } else {
                format!("{}:{}", event_lsn, xid)
            };

            events.push(ChangeEvent {
                op,
                before,
                after,
                source,
                ts_ms,
                id,
            });
        }

        // wal2json does not carry per-event LSNs, so commit_lsn is always None.
        Ok(DecodeResult {
            events,
            commit_lsn: None,
        })
    }

    fn name(&self) -> &'static str {
        "wal2json"
    }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/// Create a [`WalDecoder`] for the named output plugin.
///
/// Supported plugins:
///
/// | Name        | Implementation                    |
/// |-------------|-----------------------------------|
/// | `pgoutput`  | [`PgoutputDecoder`] (default)     |
/// | `wal2json`  | [`Wal2JsonDecoder`] (experimental) |
///
/// # Errors
///
/// Returns [`TapError::Config`] if the plugin name is not recognised.
pub fn create_decoder(plugin: &str, db_name: &str) -> Result<Box<dyn WalDecoder>, TapError> {
    match plugin {
        "pgoutput" => Ok(Box::new(PgoutputDecoder::new(db_name))),
        "wal2json" => Ok(Box::new(Wal2JsonDecoder::new(db_name))),
        other => Err(TapError::Config(format!(
            "unknown replication plugin '{other}': expected 'pgoutput' or 'wal2json'"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Binary read helpers  (big-endian, network byte order)
// ---------------------------------------------------------------------------

/// Read a single byte without advancing offset (just a peek).
#[inline]
fn read_u8(buf: &[u8], offset: &mut usize) -> Result<u8, TapError> {
    if *offset >= buf.len() {
        return Err(TapError::Decode("unexpected end of data (u8)".into()));
    }
    let val = buf[*offset];
    *offset += 1;
    Ok(val)
}

/// Read a signed 8-bit integer.
#[inline]
fn read_i8(buf: &[u8], offset: &mut usize) -> Result<i8, TapError> {
    Ok(read_u8(buf, offset)? as i8)
}

/// Read a big-endian i16.
fn read_i16(buf: &[u8], offset: &mut usize) -> Result<i16, TapError> {
    if *offset + 2 > buf.len() {
        return Err(TapError::Decode("unexpected end of data (i16)".into()));
    }
    let val = i16::from_be_bytes([buf[*offset], buf[*offset + 1]]);
    *offset += 2;
    Ok(val)
}

/// Read a big-endian i32.
fn read_i32(buf: &[u8], offset: &mut usize) -> Result<i32, TapError> {
    if *offset + 4 > buf.len() {
        return Err(TapError::Decode("unexpected end of data (i32)".into()));
    }
    let val = i32::from_be_bytes([
        buf[*offset],
        buf[*offset + 1],
        buf[*offset + 2],
        buf[*offset + 3],
    ]);
    *offset += 4;
    Ok(val)
}

/// Read a big-endian i64.
fn read_i64(buf: &[u8], offset: &mut usize) -> Result<i64, TapError> {
    if *offset + 8 > buf.len() {
        return Err(TapError::Decode("unexpected end of data (i64)".into()));
    }
    let val = i64::from_be_bytes([
        buf[*offset],
        buf[*offset + 1],
        buf[*offset + 2],
        buf[*offset + 3],
        buf[*offset + 4],
        buf[*offset + 5],
        buf[*offset + 6],
        buf[*offset + 7],
    ]);
    *offset += 8;
    Ok(val)
}

/// Read a null-terminated C string.
fn read_cstring(buf: &[u8], offset: &mut usize) -> Result<String, TapError> {
    let start = *offset;
    // Find the NUL terminator
    while *offset < buf.len() && buf[*offset] != 0 {
        *offset += 1;
    }
    if *offset >= buf.len() {
        return Err(TapError::Decode("unterminated C string".into()));
    }
    // buf[*offset] is now NUL
    let slice = &buf[start..*offset];
    *offset += 1; // skip NUL
    String::from_utf8(slice.to_vec())
        .map_err(|e| TapError::Decode(format!("invalid UTF-8 in C string: {e}")))
}

/// Read exactly `len` bytes.
fn read_bytes<'a>(buf: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8], TapError> {
    if *offset + len > buf.len() {
        return Err(TapError::Decode(format!(
            "unexpected end of data: wanted {len} bytes, have {}",
            buf.len().saturating_sub(*offset)
        )));
    }
    let slice = &buf[*offset..*offset + len];
    *offset += len;
    Ok(slice)
}

// ---------------------------------------------------------------------------
// Data conversion helpers
// ---------------------------------------------------------------------------

/// Offset from the PostgreSQL epoch (2000-01-01) to the Unix epoch
/// (1970-01-01) in **milliseconds**.
///
/// There are 10 957 days between 2000-01-01 and 1970-01-01 (accounting for
/// 7 leap days).  In milliseconds:
/// `10_957 * 86_400 * 1_000 = 946_684_800_000`
const POSTGRES_EPOCH_OFFSET_MS: i64 = 946_684_800_000;

/// Convert a pgoutput microsecond timestamp (microseconds since Postgres
/// epoch 2000-01-01) to a Unix epoch millisecond timestamp.
///
/// Returns 0 for negative timestamps (safety guard).
fn pg_timestamp_to_unix_ms(pg_us: i64) -> u64 {
    let pg_ms = pg_us / 1_000;
    pg_ms.saturating_add(POSTGRES_EPOCH_OFFSET_MS).max(0) as u64
}

/// Convert raw byte data from a pgoutput TupleData text field into a
/// [`serde_json::Value`].
///
/// Heuristic:
/// 1. Try parsing as `i64` (covers int2, int4, int8).
/// 2. Try parsing as `f64` (covers float4, float8, numeric).
/// 3. Fall back to `Value::String`.
fn raw_bytes_to_json(data: &[u8]) -> JsonValue {
    let text = std::str::from_utf8(data);
    let text = match text {
        Ok(t) => t,
        Err(_) => return JsonValue::String(hex_encode(data)),
    };

    // Try integer
    if let Ok(n) = text.parse::<i64>() {
        return JsonValue::Number(n.into());
    }

    // Try float
    if let Ok(n) = text.parse::<f64>() {
        if n.is_finite() {
            if let Some(v) = serde_json::Number::from_f64(n) {
                return JsonValue::Number(v);
            }
        }
    }

    JsonValue::String(text.to_string())
}

/// Encode bytes as a hex string prefixed with `\x` (PostgreSQL bytea hex
/// format).
fn hex_encode(data: &[u8]) -> String {
    let mut s = String::with_capacity(data.len() * 2 + 2);
    s.push_str("\\x");
    for b in data {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Parse a wal2json `"timestamp"` field (ISO 8601 format) into a Unix epoch
/// millisecond value.
///
/// wal2json emits timestamps in several forms:
///
/// | Example                                                   |
/// |-----------------------------------------------------------|
/// | `2024-05-31 12:00:00.123456+00`                          |
/// | `2024-05-31T12:00:00.123456Z`                            |
/// | `2024-05-31T12:00:00+00:00`                              |
/// | `2024-06-01T08:30:00.5Z`                                 |
///
/// Offsets may omit the colon (`+00` instead of `+00:00`) and
/// may use a space instead of `T` to separate date and time.
fn parse_wal2json_timestamp(ts: Option<&JsonValue>) -> Result<u64, TapError> {
    let ts_str = match ts {
        Some(JsonValue::String(s)) => s,
        _ => return Ok(0),
    };

    // Replace space separator with 'T' for uniform parsing.
    let normalised = ts_str.replace(' ', "T");

    // Try each format chrono can handle directly.
    // Order matters: more specific formats first.
    // Format specifiers:
    //   %+z  = +HHMM or +HH:MM or Z
    //   %f   = fractional seconds (any precision, optional)
    //   %.f  = like %f but requires leading dot
    // We use multiple format strings because chrono cannot do optional
    // fractional seconds in a single format.

    let formats: &[&str] = &[
        // With timezone offset (T separator, optional fractional)
        "%Y-%m-%dT%H:%M:%S%.f%#z",
        "%Y-%m-%dT%H:%M:%S%#z",
        // With timezone offset (no T — after space→T replacement, redundant
        // but kept for clarity):
        // Z suffix
        "%Y-%m-%dT%H:%M:%S%.fZ",
        "%Y-%m-%dT%H:%M:%SZ",
    ];

    for fmt in formats {
        if let Ok(dt) = chrono::DateTime::parse_from_str(&normalised, fmt) {
            return Ok(dt.timestamp_millis() as u64);
        }
    }

    // No timezone — assume UTC.
    let cleaned = normalised.trim_end_matches('Z');
    let naive_formats: &[&str] = &["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S"];
    for fmt in naive_formats {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(cleaned, fmt) {
            return Ok(dt.and_utc().timestamp_millis() as u64);
        }
    }

    Err(TapError::Decode(format!(
        "unable to parse wal2json timestamp: {ts_str}"
    )))
}

/// Build a JSON object from wal2json parallel arrays (`columnnames`,
/// `columntypes`, `columnvalues`).
fn build_json_from_columns(
    names: Option<&JsonValue>,
    _types: Option<&JsonValue>,
    values: Option<&JsonValue>,
) -> Result<Option<JsonValue>, TapError> {
    let names_arr = match names {
        Some(JsonValue::Array(a)) => a,
        _ => return Ok(None),
    };
    let values_arr = match values {
        Some(JsonValue::Array(a)) => a,
        _ => return Ok(Some(JsonValue::Object(serde_json::Map::new()))),
    };

    let mut map = serde_json::Map::with_capacity(names_arr.len());

    for (i, name) in names_arr.iter().enumerate() {
        let col_name = match name {
            JsonValue::String(s) => s.as_str(),
            _ => continue,
        };
        let val = values_arr.get(i).cloned().unwrap_or(JsonValue::Null);
        map.insert(col_name.to_string(), val);
    }

    Ok(Some(JsonValue::Object(map)))
}

/// Build a JSON object from a wal2json `oldkeys` structure.
///
/// `oldkeys: { "keynames": [...], "keytypes": [...], "keyvalues": [...] }`
fn build_json_from_oldkeys(oldkeys: Option<&JsonValue>) -> Result<Option<JsonValue>, TapError> {
    let obj = match oldkeys {
        Some(JsonValue::Object(o)) => o,
        _ => return Ok(None),
    };

    let keynames = match obj.get("keynames") {
        Some(JsonValue::Array(a)) => a,
        _ => return Ok(None),
    };
    let keyvalues = match obj.get("keyvalues") {
        Some(JsonValue::Array(a)) => a,
        _ => return Ok(Some(JsonValue::Object(serde_json::Map::new()))),
    };

    let mut map = serde_json::Map::with_capacity(keynames.len());

    for (i, name) in keynames.iter().enumerate() {
        let col_name = match name {
            JsonValue::String(s) => s.as_str(),
            _ => continue,
        };
        let val = keyvalues.get(i).cloned().unwrap_or(JsonValue::Null);
        map.insert(col_name.to_string(), val);
    }

    Ok(Some(JsonValue::Object(map)))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helper: build pgoutput binary messages ───────────────────────

    fn be16(v: u16) -> [u8; 2] {
        v.to_be_bytes()
    }

    fn be32(v: u32) -> [u8; 4] {
        v.to_be_bytes()
    }

    fn be64(v: u64) -> [u8; 8] {
        v.to_be_bytes()
    }

    fn cstring(s: &str) -> Vec<u8> {
        let mut bytes = s.as_bytes().to_vec();
        bytes.push(0);
        bytes
    }

    /// Build a pgoutput Begin message.
    /// `'B' | Int64 lsn | Int64 commit_time_us | Int32 xid`
    fn build_begin(lsn: u64, commit_time_us: i64, xid: u32) -> Vec<u8> {
        let mut msg = vec![b'B'];
        msg.extend_from_slice(&be64(lsn));
        msg.extend_from_slice(&be64(commit_time_us as u64));
        msg.extend_from_slice(&be32(xid));
        msg
    }

    /// Build a pgoutput Relation message.
    fn build_relation(
        relation_id: u32,
        schema: &str,
        table: &str,
        columns: &[(&str, u32, i32)], // (name, type_oid, modifier)
    ) -> Vec<u8> {
        let mut msg = vec![b'R'];
        msg.extend_from_slice(&be32(relation_id));
        msg.extend_from_slice(&cstring(schema));
        msg.extend_from_slice(&cstring(table));
        msg.push(100); // replica_identity (100 = default)
        msg.extend_from_slice(&be16(columns.len() as u16));
        for (name, typ, modifier) in columns {
            msg.push(0); // flags (0 = not part of key)
            msg.extend_from_slice(&cstring(name));
            msg.extend_from_slice(&be32(*typ));
            msg.extend_from_slice(&be32(*modifier as u32));
        }
        msg
    }

    /// Build a pgoutput Insert message with text-valued columns.
    fn build_insert(relation_id: u32, values: &[(&[u8], bool)]) -> Vec<u8> {
        // values: (raw_bytes, is_null) — if is_null, raw_bytes is ignored
        let mut msg = vec![b'I'];
        msg.extend_from_slice(&be32(relation_id));
        msg.push(b'N'); // new tuple
        msg.extend_from_slice(&be16(values.len() as u16));
        for (data, is_null) in values {
            if *is_null {
                msg.push(b'n');
            } else {
                msg.push(b't');
                msg.extend_from_slice(&be32(data.len() as u32));
                msg.extend_from_slice(data);
            }
        }
        msg
    }

    /// Build a pgoutput Update message.
    fn build_update(
        relation_id: u32,
        old_values: Option<&[(&[u8], bool)]>,
        new_values: &[(&[u8], bool)],
    ) -> Vec<u8> {
        let mut msg = vec![b'U'];
        msg.extend_from_slice(&be32(relation_id));

        if let Some(old) = old_values {
            msg.push(b'K'); // old key
            msg.extend_from_slice(&be16(old.len() as u16));
            for (data, is_null) in old {
                if *is_null {
                    msg.push(b'n');
                } else {
                    msg.push(b't');
                    msg.extend_from_slice(&be32(data.len() as u32));
                    msg.extend_from_slice(data);
                }
            }
        } else {
            msg.push(b'N'); // new tuple only
        }

        // new tuple
        msg.extend_from_slice(&be16(new_values.len() as u16));
        for (data, is_null) in new_values {
            if *is_null {
                msg.push(b'n');
            } else {
                msg.push(b't');
                msg.extend_from_slice(&be32(data.len() as u32));
                msg.extend_from_slice(data);
            }
        }
        msg
    }

    /// Build a pgoutput Delete message.
    fn build_delete(relation_id: u32, values: &[(&[u8], bool)]) -> Vec<u8> {
        let mut msg = vec![b'D'];
        msg.extend_from_slice(&be32(relation_id));
        msg.push(b'O'); // old tuple
        msg.extend_from_slice(&be16(values.len() as u16));
        for (data, is_null) in values {
            if *is_null {
                msg.push(b'n');
            } else {
                msg.push(b't');
                msg.extend_from_slice(&be32(data.len() as u32));
                msg.extend_from_slice(data);
            }
        }
        msg
    }

    /// Build a pgoutput Commit message.
    /// Format: `'C' | Int8 flags | Int64 commit_lsn | Int64 end_lsn | Int64 ts_us`
    fn build_commit(flags: u8, commit_lsn: u64, end_lsn: u64, ts_us: i64) -> Vec<u8> {
        let mut msg = vec![b'C'];
        msg.push(flags);
        msg.extend_from_slice(&be64(commit_lsn));
        msg.extend_from_slice(&be64(end_lsn));
        msg.extend_from_slice(&be64(ts_us as u64));
        msg
    }

    /// Construct a full transaction: Begin + [Relation] + DMLs + Commit.
    fn build_transaction(
        begin_lsn: u64,
        xid: u32,
        ts_us: i64,
        relation: Option<Vec<u8>>,
        dmls: Vec<Vec<u8>>,
        commit_lsn: u64,
    ) -> Vec<u8> {
        let mut msg = build_begin(begin_lsn, ts_us, xid);
        if let Some(rel) = relation {
            msg.extend_from_slice(&rel);
        }
        for dml in dmls {
            msg.extend_from_slice(&dml);
        }
        msg.extend_from_slice(&build_commit(0u8, commit_lsn, commit_lsn, ts_us));
        msg
    }

    // ── Test: pgoutput Insert ────────────────────────────────────────

    #[test]
    fn test_decode_pgoutput_insert() {
        let mut decoder = PgoutputDecoder::new("");

        let lsn = 0x16B37428u64;
        let xid = 12345u32;
        let ts_us: i64 = 0; // 2000-01-01 → 946684800000 ms

        // Build: Begin + Relation + Insert + Commit
        let msg = build_transaction(
            lsn,
            xid,
            ts_us,
            Some(build_relation(
                1,
                "public",
                "users",
                &[("id", 23, -1), ("name", 25, -1)],
            )),
            vec![build_insert(1, &[(b"42", false), (b"Alice", false)])],
            0x16B37429u64,
        );

        let events = decoder.decode(&msg).unwrap().events;

        // Wait: Begin + Relation + Insert + Commit = 4 messages.
        // But commit should flush events after processing all messages.
        // The decoder accumulates until Commit.
        // Actually decode() processes ALL messages in the buffer and
        // returns the Commit-flushed events.
        assert_eq!(events.len(), 1, "expected 1 event from commit");

        let ev = &events[0];
        assert_eq!(ev.op, Operation::Create, "insert → op='c'");
        assert!(ev.before.is_none(), "insert has no before");
        assert!(ev.after.is_some(), "insert has after");
        assert_eq!(ev.source.schema, "public");
        assert_eq!(ev.source.table, "users");
        assert_eq!(ev.source.tx_id, "12345");

        // Check after values
        let after = ev.after.as_ref().unwrap();
        assert_eq!(after.get("id").and_then(|v| v.as_i64()), Some(42));
        assert_eq!(after.get("name").and_then(|v| v.as_str()), Some("Alice"));
    }

    // ── Test: pgoutput Update ────────────────────────────────────────

    #[test]
    fn test_decode_pgoutput_update() {
        let mut decoder = PgoutputDecoder::new("");

        let msg = build_transaction(
            0x100,
            42,
            1_000_000_000i64, // ~2000-01-12
            Some(build_relation(
                1,
                "public",
                "products",
                &[("id", 23, -1), ("price", 701, -1), ("name", 25, -1)],
            )),
            vec![build_update(
                1,
                Some(&[(b"1", false)]), // old key
                &[(b"1", false), (b"1999", false), (b"Widget", false)], // new tuple
            )],
            0x200,
        );

        let events = decoder.decode(&msg).unwrap().events;
        assert_eq!(events.len(), 1);

        let ev = &events[0];
        assert_eq!(ev.op, Operation::Update);
        assert!(ev.before.is_some(), "update has before");
        assert!(ev.after.is_some(), "update has after");

        let before = ev.before.as_ref().unwrap();
        assert_eq!(before.get("id").and_then(|v| v.as_i64()), Some(1));

        let after = ev.after.as_ref().unwrap();
        assert_eq!(after.get("price").and_then(|v| v.as_i64()), Some(1999));
        assert_eq!(after.get("name").and_then(|v| v.as_str()), Some("Widget"));
    }

    // ── Test: pgoutput Delete ────────────────────────────────────────

    #[test]
    fn test_decode_pgoutput_delete() {
        let mut decoder = PgoutputDecoder::new("");

        let msg = build_transaction(
            0x300,
            7,
            2_000_000_000i64,
            Some(build_relation(
                1,
                "public",
                "orders",
                &[("id", 23, -1), ("total", 701, -1)],
            )),
            vec![build_delete(1, &[(b"99", false), (b"5000", false)])],
            0x400,
        );

        let events = decoder.decode(&msg).unwrap().events;
        assert_eq!(events.len(), 1);

        let ev = &events[0];
        assert_eq!(ev.op, Operation::Delete);
        assert!(ev.before.is_some(), "delete has before");
        assert!(ev.after.is_none(), "delete has no after");

        let before = ev.before.as_ref().unwrap();
        assert_eq!(before.get("id").and_then(|v| v.as_i64()), Some(99));
        assert_eq!(before.get("total").and_then(|v| v.as_i64()), Some(5000));
    }

    // ── Test: Begin + Insert + Commit (transaction framing) ──────────

    #[test]
    fn test_decode_pgoutput_begin_commit() {
        let mut decoder = PgoutputDecoder::new("");

        let msg = build_transaction(
            0x500,
            1001,
            3_000_000_000i64,
            Some(build_relation(1, "public", "t1", &[("val", 25, -1)])),
            vec![build_insert(1, &[(b"hello", false)])],
            0x600,
        );

        let events = decoder.decode(&msg).unwrap().events;
        assert_eq!(events.len(), 1);

        let ev = &events[0];
        assert_eq!(ev.op, Operation::Create);
        assert!(ev.id.contains(":1001"), "event id should contain tx_id");
    }

    // ── Test: Relation message populates schema cache ────────────────

    #[test]
    fn test_decode_pgoutput_relation_cache() {
        let mut decoder = PgoutputDecoder::new("");

        // Send relation first, then a transaction
        let rel = build_relation(5, "public", "cache_test", &[("col1", 23, -1)]);

        let mut msg = Vec::new();
        msg.extend_from_slice(&rel);
        // Begin + Insert + Commit referencing relation 5
        msg.extend_from_slice(&build_begin(0x10, 100_000i64, 1));
        msg.extend_from_slice(&build_insert(5, &[(b"123", false)]));
        msg.extend_from_slice(&build_commit(0u8, 0x20, 0x20, 100_000i64));

        let events = decoder.decode(&msg).unwrap().events;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source.table, "cache_test");
        assert_eq!(events[0].source.schema, "public");
        assert_eq!(
            events[0]
                .after
                .as_ref()
                .unwrap()
                .get("col1")
                .and_then(|v| v.as_i64()),
            Some(123)
        );
    }

    // ── Test: Malformed data returns error, no panic ─────────────────

    #[test]
    fn test_decode_pgoutput_malformed() {
        let mut decoder = PgoutputDecoder::new("");

        // Truncated message — single 'B' with no payload
        let result = decoder.decode(b"B");
        assert!(result.is_err(), "truncated Begin should error");
        let err = result.unwrap_err();
        assert!(err.to_string().contains("unexpected end of data"));

        // Totally garbage bytes — unknown type
        let mut decoder2 = PgoutputDecoder::new("");
        let result = decoder2.decode(&[0xFF, 0x01, 0x02]);
        // Unknown type simply returns empty vec
        assert!(result.is_ok(), "unknown type should not panic");
        assert!(result.unwrap().events.is_empty());
    }

    // ── Test: Unknown type byte is skipped ───────────────────────────

    #[test]
    fn test_decode_pgoutput_unknown_type_skipped() {
        let mut decoder = PgoutputDecoder::new("");

        // 'Y' (Type) message — known-ignorable
        let mut msg = vec![b'Y', 0, 0, 0, 10]; // type oid=10
        msg.extend_from_slice(&cstring("pg_catalog")); // nsp
        msg.extend_from_slice(&cstring("text")); // name
        // Followed by a valid transaction
        msg.extend_from_slice(&build_transaction(
            0x10,
            1,
            1_000_000i64,
            Some(build_relation(1, "public", "t", &[("a", 25, -1)])),
            vec![build_insert(1, &[(b"x", false)])],
            0x20,
        ));

        let events = decoder.decode(&msg).unwrap().events;
        assert_eq!(events.len(), 1, "still decoded after skipping Type message");
    }

    // ── Test: Multi-event transaction ────────────────────────────────

    #[test]
    fn test_multi_event_transaction() {
        let mut decoder = PgoutputDecoder::new("");

        let msg = build_transaction(
            0x50,
            200,
            5_000_000_000i64,
            Some(build_relation(
                1,
                "public",
                "multi_test",
                &[("id", 23, -1), ("val", 25, -1)],
            )),
            vec![
                build_insert(1, &[(b"1", false), (b"first", false)]),
                build_insert(1, &[(b"2", false), (b"second", false)]),
                build_update(
                    1,
                    Some(&[(b"1", false)]),
                    &[(b"1", false), (b"updated", false)],
                ),
                build_delete(1, &[(b"2", false), (b"second", false)]),
            ],
            0x60,
        );

        let events = decoder.decode(&msg).unwrap().events;
        assert_eq!(events.len(), 4, "all 4 DMLs should produce events");

        assert_eq!(events[0].op, Operation::Create);
        assert_eq!(
            events[0]
                .after
                .as_ref()
                .unwrap()
                .get("id")
                .and_then(|v| v.as_i64()),
            Some(1)
        );

        assert_eq!(events[1].op, Operation::Create);
        assert_eq!(
            events[1]
                .after
                .as_ref()
                .unwrap()
                .get("val")
                .and_then(|v| v.as_str()),
            Some("second")
        );

        assert_eq!(events[2].op, Operation::Update);
        assert!(events[2].before.is_some());
        assert_eq!(
            events[2]
                .after
                .as_ref()
                .unwrap()
                .get("val")
                .and_then(|v| v.as_str()),
            Some("updated")
        );

        assert_eq!(events[3].op, Operation::Delete);
        assert!(events[3].before.is_some());
        assert!(events[3].after.is_none());

        // All events share the same tx_id
        for ev in &events {
            assert_eq!(ev.source.tx_id, "200");
        }

        // All events share the same id format
        for ev in &events {
            assert!(ev.id.contains(":200"), "id should contain tx_id: {}", ev.id);
        }
    }

    // ── Test: Parsing column data types ──────────────────────────────

    #[test]
    fn test_pgoutput_parse_tuple_data() {
        let mut decoder = PgoutputDecoder::new("");

        // Register a schema with mixed types
        let rel = build_relation(
            1,
            "public",
            "typed_test",
            &[
                ("name", 25, -1),     // text
                ("count", 23, -1),    // int4
                ("nullable", 25, -1), // nullable text
                ("price", 701, -1),   // float8
            ],
        );

        let begin = build_begin(0x10, 1_000_000i64, 1);
        let insert = build_insert(
            1,
            &[
                (b"Widget", false), // name → string
                (b"42", false),     // count → integer
                (b"", true),        // nullable → null
                (b"19.99", false),  // price → float
            ],
        );
        let commit = build_commit(0u8, 0x20, 0x20, 1_000_000i64);

        let mut msg = Vec::new();
        msg.extend_from_slice(&rel);
        msg.extend_from_slice(&begin);
        msg.extend_from_slice(&insert);
        msg.extend_from_slice(&commit);

        let events = decoder.decode(&msg).unwrap().events;
        assert_eq!(events.len(), 1);

        let after = events[0].after.as_ref().unwrap();
        assert_eq!(after.get("name").and_then(|v| v.as_str()), Some("Widget"));
        assert_eq!(after.get("count").and_then(|v| v.as_i64()), Some(42));
        assert_eq!(after.get("nullable"), Some(&JsonValue::Null));
    }

    // ── Test: wal2json Insert ────────────────────────────────────────

    #[test]
    fn test_decode_wal2json_insert() {
        let mut decoder = Wal2JsonDecoder::new("");

        let json = r#"{
            "xid": 123,
            "timestamp": "2024-05-31 12:00:00.123456+00",
            "change": [{
                "kind": "insert",
                "schema": "public",
                "table": "users",
                "columnnames": ["id", "name"],
                "columntypes": ["int4", "text"],
                "columnvalues": [1, "Alice"]
            }]
        }"#;

        let events = decoder.decode(json.as_bytes()).unwrap().events;
        assert_eq!(events.len(), 1);

        let ev = &events[0];
        assert_eq!(ev.op, Operation::Create);
        assert!(ev.before.is_none());
        assert!(ev.after.is_some());

        let after = ev.after.as_ref().unwrap();
        assert_eq!(after.get("id").and_then(|v| v.as_i64()), Some(1));
        assert_eq!(after.get("name").and_then(|v| v.as_str()), Some("Alice"));
        assert_eq!(ev.source.schema, "public");
        assert_eq!(ev.source.table, "users");
        assert_eq!(ev.source.tx_id, "123");
    }

    // ── Test: wal2json Update with oldkeys ───────────────────────────

    #[test]
    fn test_decode_wal2json_update() {
        let mut decoder = Wal2JsonDecoder::new("");

        let json = r#"{
            "xid": 456,
            "timestamp": "2024-06-01T08:30:00.5Z",
            "change": [{
                "kind": "update",
                "schema": "public",
                "table": "products",
                "columnnames": ["id", "name", "price"],
                "columntypes": ["int4", "text", "float8"],
                "columnvalues": [1, "Super Widget", 29.99],
                "oldkeys": {
                    "keynames": ["id"],
                    "keytypes": ["int4"],
                    "keyvalues": [1]
                }
            }]
        }"#;

        let events = decoder.decode(json.as_bytes()).unwrap().events;
        assert_eq!(events.len(), 1);

        let ev = &events[0];
        assert_eq!(ev.op, Operation::Update);

        let before = ev
            .before
            .as_ref()
            .expect("update should have before (oldkeys)");
        assert_eq!(before.get("id").and_then(|v| v.as_i64()), Some(1));

        let after = ev.after.as_ref().expect("update should have after");
        assert_eq!(
            after.get("name").and_then(|v| v.as_str()),
            Some("Super Widget")
        );
        assert_eq!(after.get("price").and_then(|v| v.as_f64()), Some(29.99));
    }

    // ── Test: wal2json Delete ────────────────────────────────────────

    #[test]
    fn test_decode_wal2json_delete() {
        let mut decoder = Wal2JsonDecoder::new("");

        let json = r#"{
            "xid": 789,
            "timestamp": "2024-06-01T08:30:00Z",
            "change": [{
                "kind": "delete",
                "schema": "public",
                "table": "orders",
                "oldkeys": {
                    "keynames": ["id"],
                    "keytypes": ["int4"],
                    "keyvalues": [99]
                }
            }]
        }"#;

        let events = decoder.decode(json.as_bytes()).unwrap().events;
        assert_eq!(events.len(), 1);

        let ev = &events[0];
        assert_eq!(ev.op, Operation::Delete);
        assert!(ev.after.is_none(), "delete has no after");

        let before = ev
            .before
            .as_ref()
            .expect("delete should have before (oldkeys)");
        assert_eq!(before.get("id").and_then(|v| v.as_i64()), Some(99));
    }

    // ── Test: Factory function ───────────────────────────────────────

    #[test]
    fn test_create_decoder() {
        let pg = create_decoder("pgoutput", "test_db").unwrap();
        assert_eq!(pg.name(), "pgoutput");

        let w2j = create_decoder("wal2json", "test_db").unwrap();
        assert_eq!(w2j.name(), "wal2json");

        match create_decoder("unknown", "") {
            Err(e) => assert!(e.to_string().contains("unknown replication plugin")),
            Ok(_) => panic!("expected error for unknown plugin"),
        }
    }

    // ── Test: Empty buffer ───────────────────────────────────────────

    #[test]
    fn test_decode_empty_buffer() {
        let mut decoder = PgoutputDecoder::new("");
        let result = decoder.decode(b"").unwrap();
        assert!(result.events.is_empty());
    }

    // ── Test: Schema cache persists across calls ─────────────────────

    #[test]
    fn test_schema_cache_persists_across_decodes() {
        let mut decoder = PgoutputDecoder::new("");

        // Send relation in first call
        let rel = build_relation(99, "public", "persist_test", &[("x", 23, -1)]);
        let result = decoder.decode(&rel).unwrap();
        assert!(result.events.is_empty(), "relation produces no events");

        // Send txn referencing it in second call
        let msg = build_transaction(
            0x10,
            5,
            1_000_000i64,
            None, // no relation message this time
            vec![build_insert(99, &[(b"999", false)])],
            0x20,
        );
        let events = decoder.decode(&msg).unwrap().events;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source.table, "persist_test");
    }

    // ── Test: Relation without matching DML ──────────────────────────

    #[test]
    fn test_relation_only_message() {
        let mut decoder = PgoutputDecoder::new("");

        let rel = build_relation(42, "public", "just_schema", &[("a", 25, -1)]);
        let result = decoder.decode(&rel).unwrap();
        assert!(result.events.is_empty());
    }

    // ── Test: DML without prior relation returns error ───────────────

    #[test]
    fn test_dml_without_relation_errors() {
        let mut decoder = PgoutputDecoder::new("");

        let msg = build_transaction(
            0x10,
            1,
            1_000_000i64,
            None,
            vec![build_insert(999, &[(b"val", false)])],
            0x20,
        );

        let result = decoder.decode(&msg);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("no schema cache entry")
        );
    }

    // ── Test: wal2json with unknown kind is skipped ──────────────────

    #[test]
    fn test_wal2json_unknown_kind_skipped() {
        let mut decoder = Wal2JsonDecoder::new("");

        let json = r#"{
            "xid": 1,
            "timestamp": "2024-01-01T00:00:00Z",
            "change": [
                {"kind": "unknown", "schema": "public", "table": "t"},
                {"kind": "insert", "schema": "public", "table": "t",
                 "columnnames": ["id"], "columntypes": ["int4"], "columnvalues": [1]}
            ]
        }"#;

        let events = decoder.decode(json.as_bytes()).unwrap().events;
        assert_eq!(events.len(), 1, "unknown change kind should be skipped");
        assert_eq!(events[0].op, Operation::Create);
    }

    // ── Test: pgoutput timestamp conversion ──────────────────────────

    #[test]
    fn test_pg_timestamp_conversion() {
        // 0 microseconds since Postgres epoch = 2000-01-01 00:00:00 UTC
        // = 946684800000 ms since Unix epoch
        assert_eq!(pg_timestamp_to_unix_ms(0), 946_684_800_000);

        // 1 second = 1_000_000 microseconds
        assert_eq!(pg_timestamp_to_unix_ms(1_000_000), 946_684_801_000);

        // 1 hour = 3_600_000_000 microseconds
        assert_eq!(pg_timestamp_to_unix_ms(3_600_000_000), 946_688_400_000);
    }

    // ── Test: wal2json timestamp parsing ─────────────────────────────

    #[test]
    fn test_wal2json_timestamp_parsing() {
        // RFC 3339 with Z
        let val = JsonValue::String("2024-05-31T12:00:00.123456Z".into());
        let ms = parse_wal2json_timestamp(Some(&val)).unwrap();
        assert_eq!(ms, 1_717_156_800_123);

        // RFC 3339 with offset — ".5" means 500 ms
        let val = JsonValue::String("2024-05-31T12:00:00.5+00:00".into());
        let ms = parse_wal2json_timestamp(Some(&val)).unwrap();
        assert_eq!(ms, 1_717_156_800_500);

        // wal2json format with space
        let val = JsonValue::String("2024-05-31 12:00:00.123456+00".into());
        let ms = parse_wal2json_timestamp(Some(&val)).unwrap();
        assert_eq!(ms, 1_717_156_800_123);

        // Missing timestamp → returns 0
        assert_eq!(parse_wal2json_timestamp(None).unwrap(), 0);
    }

    // ── Test: hex_encode for binary data ─────────────────────────────

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(b""), "\\x");
        assert_eq!(hex_encode(b"\x00\x01\xFF"), "\\x0001ff");
        assert_eq!(hex_encode(b"hello"), "\\x68656c6c6f");
    }

    // ── Test: raw_bytes_to_json heuristics ───────────────────────────

    #[test]
    fn test_raw_bytes_to_json() {
        // Integer
        assert_eq!(raw_bytes_to_json(b"42"), JsonValue::Number(42.into()));
        assert_eq!(raw_bytes_to_json(b"-7"), JsonValue::Number((-7).into()));

        // Float
        let f = raw_bytes_to_json(b"3.14");
        assert!(
            matches!(f, JsonValue::Number(ref n) if (n.as_f64().unwrap() - 3.14).abs() < 0.001),
            "expected ~3.14, got {f:?}"
        );

        // String
        assert_eq!(
            raw_bytes_to_json(b"hello world"),
            JsonValue::String("hello world".into())
        );

        // Binary (not valid UTF-8) → hex
        let bin = raw_bytes_to_json(b"\xFF\xFE\x00");
        assert_eq!(bin, JsonValue::String("\\xfffe00".into()));
    }

    // ── Test: Truncate and Origin messages are skipped ───────────────

    #[test]
    fn test_skip_truncate_and_origin() {
        let mut decoder = PgoutputDecoder::new("");

        // Build a message with 't' (Truncate), 'o' (Origin), and a valid txn
        let mut msg = Vec::new();

        // Truncate: 'T' | Int32 nrels | Int32 relids[nrels] | Byte1 options
        msg.push(b'T');
        msg.extend_from_slice(&be32(1)); // 1 relation
        msg.extend_from_slice(&be32(42)); // relation 42
        msg.push(0); // options = 0 (cascade)

        // Origin: 'O' | String name | Int64 lsn
        msg.push(b'O');
        msg.extend_from_slice(&cstring("test_origin"));
        msg.extend_from_slice(&be64(0x100));

        // Valid transaction
        msg.extend_from_slice(&build_transaction(
            0x10,
            1,
            1_000_000i64,
            Some(build_relation(1, "public", "t", &[("a", 25, -1)])),
            vec![build_insert(1, &[(b"x", false)])],
            0x20,
        ));

        let events = decoder.decode(&msg).unwrap().events;
        assert_eq!(events.len(), 1, "skipped Truncate + Origin, decoded txn");
    }

    // ── Test: wal2json with invalid JSON ─────────────────────────────

    #[test]
    fn test_wal2json_invalid_json() {
        let mut decoder = Wal2JsonDecoder::new("");
        let result = decoder.decode(b"not valid json");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("parse error"));
    }

    // ── Test: wal2json with missing change array ─────────────────────

    #[test]
    fn test_wal2json_missing_change() {
        let mut decoder = Wal2JsonDecoder::new("");
        let result = decoder.decode(b"{}");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing 'change'"));
    }
}
