//! [`StateStore`] — SQLite-backed persistence for replication state.
//!
//! Uses WAL-mode rusqlite to store checkpoints, snapshot progress, cached
//! schemas, skipped LSNs, and instance metadata in six tables.  The store
//! performs an integrity check and optional backup on open, and acquires an
//! exclusive transaction to detect duplicate instances.

use std::path::Path;

use rusqlite::{Connection, params};
use tracing::info;

use crate::config::StateConfig;
use crate::error::TapError;
use crate::postgres::Lsn;

use super::migration::migrate;

// ---------------------------------------------------------------------------
// Record types
// ---------------------------------------------------------------------------

/// A single committed offset (checkpoint) read from the store.
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetRecord {
    /// The position string (e.g. `"0/16B37428"` for Postgres LSN).
    pub position: String,
    /// Identifier of the transaction that produced this offset.
    pub tx_id: String,
    /// Wall-clock timestamp (milliseconds) when this offset was committed.
    pub ts_ms: u64,
    /// Monotonically increasing sequence number for ordering offsets.
    pub sequence: i64,
    /// Whether this offset is a final (flush) marker.
    pub is_final: bool,
}

/// Snapshot progress record for a single table.
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotRecord {
    /// Schema-qualified table name.
    pub table_name: String,
    /// Unique identifier for the snapshot run.
    pub snapshot_id: String,
    /// Number of rows processed so far (or total on completion).
    pub rows_count: u64,
    /// Status: `"in_progress"`, `"completed"`, or `"failed"`.
    pub status: String,
}

/// Cached table schema record.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaRecord {
    /// Schema-qualified table name.
    pub table_name: String,
    /// JSON blob of column definitions.
    pub columns_json: String,
    /// Column names that form the primary key (parsed from JSON).
    pub primary_keys: Vec<String>,
    /// Hash that changes when the schema changes.
    pub schema_hash: String,
}

/// A single snapshot-chunk row from the state store:
/// `(chunk_index, chunk_start, chunk_end, status)`.
pub type ChunkRow = (u32, Option<String>, Option<String>, String);

// ---------------------------------------------------------------------------
// StateStore
// ---------------------------------------------------------------------------

/// SQLite-backed state store for replication checkpoints and metadata.
///
/// Opens a single WAL-mode SQLite connection.  The struct is **not** `Send`
/// or `Sync` because [`rusqlite::Connection`] is not thread-safe.  Share
/// across tasks via `Mutex<StateStore>` in `Arc`.
///
/// # Example
///
/// ```rust,no_run
/// use tap_core::config::StateConfig;
/// use tap_core::state::StateStore;
///
/// let config = StateConfig::default();
/// let store = StateStore::open(&config).unwrap();
/// let offset = store.read_last_offset().unwrap();
/// println!("last offset: {offset:?}");
/// ```
pub struct StateStore {
    conn: Connection,
    #[allow(dead_code)]
    config: StateConfig,
}

impl StateStore {
    /// Open (or create) the SQLite database at the configured path.
    ///
    /// The open sequence performs:
    /// 1. Open or create the database file (creating parent dirs if needed).
    /// 2. Apply WAL-mode and safety pragmas.
    /// 3. Run `PRAGMA integrity_check`.
    /// 4. Create a backup copy (`state.db.bak`) if the database already
    ///    existed (skipped for brand-new databases).
    /// 5. Run schema migrations to bring the database up to date.
    /// 6. Briefly acquire and release a `BEGIN EXCLUSIVE` transaction to
    ///    detect duplicate instances.
    ///
    /// # Errors
    ///
    /// Returns [`TapError::Io`] if the database directory cannot be created.
    /// Returns [`TapError::Sqlite`] for connection or pragma errors.
    /// Returns [`TapError::StateCorruption`] if the integrity check fails.
    pub fn open(config: &StateConfig) -> Result<Self, TapError> {
        let path = Path::new(&config.path);

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Check whether the DB file already exists — we only back up
        // pre-existing databases, not freshly created ones.
        let db_existed = path.exists();

        let mut conn = Connection::open(path)?;

        // ---- Pragma setup ----
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;
             PRAGMA synchronous = NORMAL;",
        )?;

        // ---- Integrity check ----
        {
            let mut stmt = conn.prepare("PRAGMA integrity_check")?;
            let result: String = stmt.query_row([], |row| row.get(0))?;
            if result != "ok" {
                return Err(TapError::StateCorruption(format!(
                    "integrity_check failed: {result}"
                )));
            }
        }

        // ---- Backup pre-existing database (best-effort) ----
        if db_existed {
            let bak = path.with_extension("db.bak");
            if let Err(e) = std::fs::copy(path, &bak) {
                info!(from = %path.display(), to = %bak.display(), "backup skipped: {e}");
            }
        }

        // ---- Run migrations ----
        migrate(&mut conn)?;

        // ---- Exclusive lock to detect duplicate instances ----
        // Acquire an exclusive transaction briefly.  If another instance
        // already holds a lock, this will block up to busy_timeout (5 s)
        // and then fail with a SQLITE_BUSY error.
        conn.execute_batch("BEGIN EXCLUSIVE TRANSACTION")?;
        conn.execute_batch("COMMIT")?;

        info!(path = %path.display(), "state store opened");
        Ok(Self {
            conn,
            config: config.clone(),
        })
    }

    // -----------------------------------------------------------------------
    // Offset operations
    // -----------------------------------------------------------------------

    /// Persist a committed offset checkpoint.
    ///
    /// `adapter` identifies the replication plugin (e.g. `"pgoutput"`).
    /// It is stored alongside the position for connector-agnostic tracking.
    pub fn write_offset(
        &self,
        lsn: &Lsn,
        tx_id: &str,
        ts_ms: u64,
        is_final: bool,
        adapter: &str,
    ) -> Result<(), TapError> {
        let lsn_str = lsn.to_string();
        // Compute next sequence number
        let sequence: i64 = self.conn.query_row(
            "SELECT COALESCE(MAX(sequence), 0) + 1 FROM offsets",
            [],
            |row| row.get(0),
        )?;

        self.conn.execute(
            "INSERT INTO offsets (position, tx_id, ts_ms, sequence, is_final, adapter)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                lsn_str,
                tx_id,
                ts_ms as i64,
                sequence,
                is_final as i64,
                adapter
            ],
        )?;
        Ok(())
    }

    /// Read the most recent offset checkpoint.
    ///
    /// Returns `None` when no offsets have ever been written.
    pub fn read_last_offset(&self) -> Result<Option<OffsetRecord>, TapError> {
        // Prefer the latest final offset (resume from a clean checkpoint).
        // Fall back to the highest sequence overall.
        let result = self.conn.query_row(
            "SELECT position, tx_id, ts_ms, sequence, is_final
                 FROM offsets
                 WHERE is_final = 1
                 ORDER BY sequence DESC
                 LIMIT 1",
            [],
            |row| {
                Ok(OffsetRecord {
                    position: row.get(0)?,
                    tx_id: row.get(1)?,
                    ts_ms: row.get::<_, i64>(2)? as u64,
                    sequence: row.get(3)?,
                    is_final: row.get::<_, i64>(4)? != 0,
                })
            },
        );

        match result {
            Ok(record) => Ok(Some(record)),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                // Fallback: latest by sequence, regardless of is_final
                let result = self.conn.query_row(
                    "SELECT position, tx_id, ts_ms, sequence, is_final
                     FROM offsets
                     ORDER BY sequence DESC
                     LIMIT 1",
                    [],
                    |row| {
                        Ok(OffsetRecord {
                            position: row.get(0)?,
                            tx_id: row.get(1)?,
                            ts_ms: row.get::<_, i64>(2)? as u64,
                            sequence: row.get(3)?,
                            is_final: row.get::<_, i64>(4)? != 0,
                        })
                    },
                );
                match result {
                    Ok(record) => Ok(Some(record)),
                    Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                    Err(e) => Err(e.into()),
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Remove all offset records (for testing or reset).
    pub fn clear_offsets(&self) -> Result<(), TapError> {
        self.conn.execute("DELETE FROM offsets", [])?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Snapshot operations
    // -----------------------------------------------------------------------

    /// Record or update snapshot progress for a table.
    pub fn write_snapshot_progress(
        &self,
        table: &str,
        snapshot_id: &str,
        rows: u64,
        snapshot_lsn: &Lsn,
    ) -> Result<(), TapError> {
        self.conn.execute(
            "INSERT INTO snapshots (table_name, snapshot_id, rows_count, status, snapshot_lsn)
             VALUES (?1, ?2, ?3, 'in_progress', ?4)
             ON CONFLICT(table_name, snapshot_id) DO UPDATE SET
               rows_count = ?3,
               status = CASE WHEN status = 'completed' THEN status ELSE 'in_progress' END",
            params![table, snapshot_id, rows as i64, snapshot_lsn.to_string()],
        )?;
        Ok(())
    }

    /// Mark a snapshot as completed.
    pub fn complete_snapshot(
        &self,
        table: &str,
        snapshot_id: &str,
        rows: u64,
    ) -> Result<(), TapError> {
        self.conn.execute(
            "UPDATE snapshots
             SET status = 'completed',
                 rows_count = ?3,
                 completed_at = datetime('now')
             WHERE table_name = ?1 AND snapshot_id = ?2",
            params![table, snapshot_id, rows as i64],
        )?;
        Ok(())
    }

    /// Get the current snapshot status for a table, returning the most
    /// recent snapshot record.
    pub fn get_snapshot_status(&self, table: &str) -> Result<Option<SnapshotRecord>, TapError> {
        let result = self.conn.query_row(
            "SELECT table_name, snapshot_id, rows_count, status
             FROM snapshots
             WHERE table_name = ?1
             ORDER BY started_at DESC
             LIMIT 1",
            params![table],
            |row| {
                Ok(SnapshotRecord {
                    table_name: row.get(0)?,
                    snapshot_id: row.get(1)?,
                    rows_count: row.get::<_, i64>(2)? as u64,
                    status: row.get(3)?,
                })
            },
        );

        match result {
            Ok(record) => Ok(Some(record)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    // -----------------------------------------------------------------------
    // Schema cache operations
    // -----------------------------------------------------------------------

    /// Cache or update the schema for a table.
    ///
    /// `columns_json` should be a JSON-encoded array of column definitions.
    /// `primary_keys` is a slice of column names that form the primary key.
    /// `schema_hash` should change whenever the schema changes.
    pub fn cache_schema(
        &self,
        table: &str,
        columns_json: &str,
        primary_keys: &[String],
        schema_hash: &str,
    ) -> Result<(), TapError> {
        let pk_json = serde_json::to_string(primary_keys).map_err(|e| {
            TapError::StateCorruption(format!("failed to serialize primary_keys: {e}"))
        })?;

        self.conn.execute(
            "INSERT INTO schema_cache (table_name, columns_json, primary_keys, schema_hash)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(table_name) DO UPDATE SET
               columns_json = ?2,
               primary_keys = ?3,
               schema_hash = ?4,
               schema_version = schema_version + 1,
               updated_at = datetime('now')",
            params![table, columns_json, pk_json, schema_hash],
        )?;
        Ok(())
    }

    /// Retrieve the cached schema for a table, if any.
    pub fn get_cached_schema(&self, table: &str) -> Result<Option<SchemaRecord>, TapError> {
        let result = self.conn.query_row(
            "SELECT table_name, columns_json, primary_keys, schema_hash
             FROM schema_cache
             WHERE table_name = ?1",
            params![table],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        );

        match result {
            Ok((table_name, columns_json, pks_str, schema_hash)) => {
                let primary_keys: Vec<String> = serde_json::from_str(&pks_str).map_err(|e| {
                    TapError::StateCorruption(format!(
                        "corrupted primary_keys JSON for table '{table}': {e}"
                    ))
                })?;
                Ok(Some(SchemaRecord {
                    table_name,
                    columns_json,
                    primary_keys,
                    schema_hash,
                }))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    // -----------------------------------------------------------------------
    // Chunk operations (parallel snapshot)
    // -----------------------------------------------------------------------

    /// Insert a new chunk record for a parallel snapshot.
    pub fn write_chunk(
        &self,
        table_name: &str,
        snapshot_id: &str,
        chunk_index: u32,
        chunk_start: Option<&str>,
        chunk_end: Option<&str>,
    ) -> Result<(), TapError> {
        self.conn.execute(
            "INSERT INTO snapshot_chunks
                (table_name, snapshot_id, chunk_index, chunk_start, chunk_end, status)
             VALUES (?1, ?2, ?3, ?4, ?5, 'pending')
             ON CONFLICT(snapshot_id, table_name, chunk_index) DO NOTHING",
            params![table_name, snapshot_id, chunk_index, chunk_start, chunk_end],
        )?;
        Ok(())
    }

    /// Mark a chunk as in_progress and set the started_at timestamp.
    pub fn start_chunk(
        &self,
        snapshot_id: &str,
        table_name: &str,
        chunk_index: u32,
    ) -> Result<(), TapError> {
        self.conn.execute(
            "UPDATE snapshot_chunks
             SET status = 'in_progress',
                 started_at = datetime('now')
             WHERE snapshot_id = ?1 AND table_name = ?2 AND chunk_index = ?3",
            params![snapshot_id, table_name, chunk_index],
        )?;
        Ok(())
    }

    /// Update the row count for a chunk (intermediate progress).
    pub fn update_chunk_rows(
        &self,
        snapshot_id: &str,
        table_name: &str,
        chunk_index: u32,
        rows_count: u64,
    ) -> Result<(), TapError> {
        self.conn.execute(
            "UPDATE snapshot_chunks
             SET rows_count = ?4
             WHERE snapshot_id = ?1 AND table_name = ?2 AND chunk_index = ?3",
            params![snapshot_id, table_name, chunk_index, rows_count as i64],
        )?;
        Ok(())
    }

    /// Mark a chunk as completed with final row count.
    pub fn complete_chunk(
        &self,
        snapshot_id: &str,
        table_name: &str,
        chunk_index: u32,
        rows_count: u64,
    ) -> Result<(), TapError> {
        self.conn.execute(
            "UPDATE snapshot_chunks
             SET status = 'completed',
                 rows_count = ?4,
                 completed_at = datetime('now')
             WHERE snapshot_id = ?1 AND table_name = ?2 AND chunk_index = ?3",
            params![snapshot_id, table_name, chunk_index, rows_count as i64],
        )?;
        Ok(())
    }

    /// Mark a chunk as failed with an error message.
    pub fn fail_chunk(
        &self,
        snapshot_id: &str,
        table_name: &str,
        chunk_index: u32,
        error_message: &str,
    ) -> Result<(), TapError> {
        self.conn.execute(
            "UPDATE snapshot_chunks
             SET status = 'failed',
                 error_message = ?4,
                 completed_at = datetime('now')
             WHERE snapshot_id = ?1 AND table_name = ?2 AND chunk_index = ?3",
            params![snapshot_id, table_name, chunk_index, error_message],
        )?;
        Ok(())
    }

    /// Get all chunks for a snapshot+table (any status).
    /// Returns `(chunk_index, chunk_start, chunk_end, status)` tuples.
    pub fn get_table_chunks(
        &self,
        snapshot_id: &str,
        table_name: &str,
    ) -> Result<Vec<ChunkRow>, TapError> {
        let mut stmt = self.conn.prepare(
            "SELECT chunk_index, chunk_start, chunk_end, status
             FROM snapshot_chunks
             WHERE snapshot_id = ?1 AND table_name = ?2
             ORDER BY chunk_index",
        )?;
        let rows = stmt
            .query_map(params![snapshot_id, table_name], |row| {
                Ok((
                    row.get::<_, i32>(0)? as u32,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Count completed chunks for a snapshot.
    pub fn count_completed_chunks(&self, snapshot_id: &str) -> Result<u64, TapError> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM snapshot_chunks
             WHERE snapshot_id = ?1 AND status = 'completed'",
            params![snapshot_id],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Count chunks that are NOT completed for a snapshot run.
    ///
    /// Returns 0 when the run is either fully completed or has no chunks
    /// at all (both cases mean "no resume needed").
    pub fn count_incomplete_chunks(&self, snapshot_id: &str) -> Result<u64, TapError> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM snapshot_chunks
             WHERE snapshot_id = ?1 AND status != 'completed'",
            params![snapshot_id],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Get the total row count across all completed chunks for a snapshot.
    pub fn snapshot_chunk_total_rows(&self, snapshot_id: &str) -> Result<u64, TapError> {
        let total: i64 = self.conn.query_row(
            "SELECT COALESCE(SUM(rows_count), 0) FROM snapshot_chunks
             WHERE snapshot_id = ?1 AND status = 'completed'",
            params![snapshot_id],
            |row| row.get(0),
        )?;
        Ok(total as u64)
    }

    // -----------------------------------------------------------------------
    // Skipped LSNs
    // -----------------------------------------------------------------------

    /// Record a position that could not be processed (previously "skipped LSN").
    pub fn record_skipped_lsn(
        &self,
        position: &str,
        tx_id: &str,
        error_message: &str,
    ) -> Result<(), TapError> {
        self.conn.execute(
            "INSERT INTO skipped_positions (position, tx_id, error_message)
             VALUES (?1, ?2, ?3)",
            params![position, tx_id, error_message],
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Instance info (key-value metadata)
    // -----------------------------------------------------------------------

    /// Set a key–value pair in instance metadata.
    pub fn set_instance_info(&self, key: &str, value: &str) -> Result<(), TapError> {
        self.conn.execute(
            "INSERT INTO instance_info (key, value, updated_at)
             VALUES (?1, ?2, datetime('now'))
             ON CONFLICT(key) DO UPDATE SET
               value = ?2,
               updated_at = datetime('now')",
            params![key, value],
        )?;
        Ok(())
    }

    /// Get a value from instance metadata by key.
    pub fn get_instance_info(&self, key: &str) -> Result<Option<String>, TapError> {
        let result = self.conn.query_row(
            "SELECT value FROM instance_info WHERE key = ?1",
            params![key],
            |row| row.get(0),
        );

        match result {
            Ok(val) => Ok(Some(val)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    // -----------------------------------------------------------------------
    // Maintenance
    // -----------------------------------------------------------------------

    /// Run `PRAGMA integrity_check` and return whether the database is OK.
    pub fn integrity_check(&self) -> Result<bool, TapError> {
        let result: String = self
            .conn
            .prepare("PRAGMA integrity_check")?
            .query_row([], |row| row.get(0))?;
        Ok(result == "ok")
    }

    /// Close the database connection explicitly.
    ///
    /// After calling this, the store should not be used again.
    pub fn close(self) -> Result<(), TapError> {
        // Drop the connection — rusqlite handles cleanup.
        drop(self.conn);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::Lsn;
    use std::str::FromStr;

    /// Helper: create a temporary StateConfig pointing at a unique file.
    fn temp_config(name: &str) -> StateConfig {
        let dir = std::env::temp_dir().join(format!("tap_test_{name}"));
        let _ = std::fs::create_dir_all(&dir);
        StateConfig {
            path: dir.join("state.db").to_string_lossy().to_string(),
            max_backup_size_kb: 10_240,
        }
    }

    /// Helper: clean up the temp directory after a test.
    fn cleanup(config: &StateConfig) {
        let path = Path::new(&config.path);
        if let Some(parent) = path.parent() {
            let _ = std::fs::remove_dir_all(parent);
        }
    }

    // ------------------------------------------------------------------
    // Test 1: Store initialisation creates all 7 tables
    // ------------------------------------------------------------------

    #[test]
    fn test_store_initialize_creates_tables() {
        let config = temp_config("init_tables");
        let store = StateStore::open(&config).expect("open store");

        // Query the list of table names from sqlite_master
        let mut stmt = store
            .conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap();
        let tables: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert!(
            tables.contains(&"instance_info".into()),
            "missing instance_info: {tables:?}"
        );
        assert!(
            tables.contains(&"offsets".into()),
            "missing offsets: {tables:?}"
        );
        assert!(
            tables.contains(&"schema_cache".into()),
            "missing schema_cache: {tables:?}"
        );
        assert!(
            tables.contains(&"schema_version".into()),
            "missing schema_version: {tables:?}"
        );
        assert!(
            tables.contains(&"skipped_positions".into()),
            "missing skipped_positions: {tables:?}"
        );
        assert!(
            tables.contains(&"snapshots".into()),
            "missing snapshots: {tables:?}"
        );
        assert!(
            tables.contains(&"snapshot_chunks".into()),
            "missing snapshot_chunks: {tables:?}"
        );

        cleanup(&config);
    }

    // ------------------------------------------------------------------
    // Test 2: Write and read offset round-trip
    // ------------------------------------------------------------------

    #[test]
    fn test_store_write_and_read_offset() {
        let config = temp_config("write_read_offset");
        let store = StateStore::open(&config).expect("open store");

        let lsn = Lsn::from_str("0/ABCD1234").unwrap();
        store
            .write_offset(&lsn, "tx_001", 1_700_000_000_000, true, "pgoutput")
            .expect("write offset");

        let record = store
            .read_last_offset()
            .expect("read offset")
            .expect("offset exists");

        assert_eq!(record.position, "0/ABCD1234");
        assert_eq!(record.tx_id, "tx_001");
        assert_eq!(record.ts_ms, 1_700_000_000_000);
        assert!(record.is_final);

        cleanup(&config);
    }

    // ------------------------------------------------------------------
    // Test 3: Multiple offsets, read the latest by sequence
    // ------------------------------------------------------------------

    #[test]
    fn test_store_multiple_offsets() {
        let config = temp_config("multiple_offsets");
        let store = StateStore::open(&config).expect("open store");

        store
            .write_offset(
                &Lsn::from_str("0/11111111").unwrap(),
                "tx_1",
                1000,
                false,
                "pgoutput",
            )
            .expect("write offset 1");
        store
            .write_offset(
                &Lsn::from_str("0/22222222").unwrap(),
                "tx_2",
                2000,
                false,
                "pgoutput",
            )
            .expect("write offset 2");
        store
            .write_offset(
                &Lsn::from_str("0/33333333").unwrap(),
                "tx_3",
                3000,
                true,
                "pgoutput",
            )
            .expect("write offset 3");

        let record = store
            .read_last_offset()
            .expect("read offset")
            .expect("offset exists");

        // Should return the final offset (is_final=1) with highest sequence
        assert_eq!(record.position, "0/33333333");
        assert_eq!(record.tx_id, "tx_3");
        assert!(record.is_final);

        cleanup(&config);
    }

    // ------------------------------------------------------------------
    // Test 4: read_last_offset returns max when no final offset
    // ------------------------------------------------------------------

    #[test]
    fn test_store_last_offset() {
        let config = temp_config("last_offset");
        let store = StateStore::open(&config).expect("open store");

        store
            .write_offset(
                &Lsn::from_str("0/AAAAAAAA").unwrap(),
                "tx_a",
                1000,
                false,
                "pgoutput",
            )
            .expect("write offset A");
        store
            .write_offset(
                &Lsn::from_str("0/BBBBBBBB").unwrap(),
                "tx_b",
                2000,
                false,
                "pgoutput",
            )
            .expect("write offset B");

        // No is_final=1 offsets — should fall back to max sequence
        let record = store
            .read_last_offset()
            .expect("read offset")
            .expect("offset exists");

        assert_eq!(record.position, "0/BBBBBBBB");

        cleanup(&config);
    }

    // ------------------------------------------------------------------
    // Test 5: Fresh DB returns None
    // ------------------------------------------------------------------

    #[test]
    fn test_store_no_offset_returns_none() {
        let config = temp_config("no_offset");
        let store = StateStore::open(&config).expect("open store");

        let result = store.read_last_offset().expect("read last offset");
        assert!(result.is_none(), "expected None, got {result:?}");

        cleanup(&config);
    }

    // ------------------------------------------------------------------
    // Test 6: Snapshot progress write/read
    // ------------------------------------------------------------------

    #[test]
    fn test_store_snapshot_progress() {
        let config = temp_config("snap_progress");
        let store = StateStore::open(&config).expect("open store");
        let lsn = Lsn::from_str("0/DEADBEEF").unwrap();

        store
            .write_snapshot_progress("public.users", "snap_1", 500, &lsn)
            .expect("write snap progress");

        let status = store
            .get_snapshot_status("public.users")
            .expect("get status")
            .expect("status exists");

        assert_eq!(status.table_name, "public.users");
        assert_eq!(status.snapshot_id, "snap_1");
        assert_eq!(status.rows_count, 500);
        assert_eq!(status.status, "in_progress");

        cleanup(&config);
    }

    // ------------------------------------------------------------------
    // Test 7: Snapshot completion (status transition)
    // ------------------------------------------------------------------

    #[test]
    fn test_store_snapshot_completion() {
        let config = temp_config("snap_complete");
        let store = StateStore::open(&config).expect("open store");
        let lsn = Lsn::from_str("0/BEEF0002").unwrap();

        store
            .write_snapshot_progress("public.orders", "snap_2", 100, &lsn)
            .expect("write progress");
        store
            .complete_snapshot("public.orders", "snap_2", 1000)
            .expect("complete snapshot");

        let status = store
            .get_snapshot_status("public.orders")
            .expect("get status")
            .expect("status exists");

        assert_eq!(status.status, "completed");
        assert_eq!(status.rows_count, 1000);

        cleanup(&config);
    }

    // ------------------------------------------------------------------
    // Test 8: Schema cache get/set round-trip
    // ------------------------------------------------------------------

    #[test]
    fn test_store_schema_cache_get_set() {
        let config = temp_config("schema_cache");
        let store = StateStore::open(&config).expect("open store");

        let columns = r#"[{"name":"id","type":"int4"},{"name":"name","type":"text"}]"#;
        let pks = vec!["id".to_string()];
        let hash = "abc123";

        store
            .cache_schema("public.users", columns, &pks, hash)
            .expect("cache schema");

        let cached = store
            .get_cached_schema("public.users")
            .expect("get cached")
            .expect("schema exists");

        assert_eq!(cached.table_name, "public.users");
        assert_eq!(cached.columns_json, columns);
        assert_eq!(cached.primary_keys, vec!["id"]);
        assert_eq!(cached.schema_hash, "abc123");

        cleanup(&config);
    }

    // ------------------------------------------------------------------
    // Test 9: Schema hash change is detected
    // ------------------------------------------------------------------

    #[test]
    fn test_store_schema_hash_detects_change() {
        let config = temp_config("schema_hash");
        let store = StateStore::open(&config).expect("open store");

        let columns = r#"[{"name":"id","type":"int4"}]"#;
        let pks = vec!["id".to_string()];

        store
            .cache_schema("public.t", columns, &pks, "hash_v1")
            .expect("cache v1");

        let v1 = store
            .get_cached_schema("public.t")
            .expect("get v1")
            .expect("v1 exists");
        assert_eq!(v1.schema_hash, "hash_v1");

        // Overwrite with new hash — simulates a schema change
        store
            .cache_schema("public.t", columns, &pks, "hash_v2")
            .expect("cache v2");

        let v2 = store
            .get_cached_schema("public.t")
            .expect("get v2")
            .expect("v2 exists");
        assert_eq!(v2.schema_hash, "hash_v2");
        assert_ne!(v1.schema_hash, v2.schema_hash);

        cleanup(&config);
    }

    // ------------------------------------------------------------------
    // Test 10: Integrity check on clean DB
    // ------------------------------------------------------------------

    #[test]
    fn test_store_integrity_check_on_corrupt_db() {
        let config = temp_config("integrity");
        // Open once to create the file
        {
            let store = StateStore::open(&config).expect("open store");
            assert!(store.integrity_check().expect("integrity check"));
        }

        // Open a second connection to corrupt the database
        {
            let conn = Connection::open(Path::new(&config.path)).expect("open raw for corruption");
            // Write garbage into the file via raw bytes
            conn.execute_batch("PRAGMA journal_mode=DELETE").ok();
        }
        // Manually corrupt the file by writing garbage
        {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(Path::new(&config.path))
                .expect("open db file");
            use std::io::Write;
            // Overwrite the first few bytes (header)
            file.write_all(b"CORRUPTED!!").expect("write garbage");
            file.flush().expect("flush");
        }

        // The store should fail to open due to corruption (or pass
        // integrity_check depending on the nature of corruption).
        // rusqlite may also fail to open the file at all.
        let result = StateStore::open(&config);
        match result {
            Ok(store) => {
                // If it opens, integrity_check should return false
                assert!(!store.integrity_check().expect("integrity check failed"));
            }
            Err(e) => {
                // Either Sqlite or StateCorruption is acceptable
                assert!(
                    matches!(&e, TapError::Sqlite(_) | TapError::StateCorruption(_)),
                    "unexpected error: {e}"
                );
            }
        }

        cleanup(&config);
    }

    // ------------------------------------------------------------------
    // Test 11: Instance info set/get
    // ------------------------------------------------------------------

    #[test]
    fn test_store_instance_info() {
        let config = temp_config("instance_info");
        let store = StateStore::open(&config).expect("open store");

        // Key not found
        assert!(
            store
                .get_instance_info("nonexistent")
                .expect("get missing")
                .is_none()
        );

        // Set and get
        store
            .set_instance_info("hostname", "myserver")
            .expect("set hostname");
        assert_eq!(
            store.get_instance_info("hostname").expect("get hostname"),
            Some("myserver".into())
        );

        // Overwrite
        store
            .set_instance_info("hostname", "newserver")
            .expect("set hostname again");
        assert_eq!(
            store.get_instance_info("hostname").expect("get hostname"),
            Some("newserver".into())
        );

        cleanup(&config);
    }

    // ------------------------------------------------------------------
    // Test 12: Migration from an empty database (version 0 → version 1)
    // ------------------------------------------------------------------

    #[test]
    fn test_store_migration_from_version_0() {
        let config = temp_config("migration_v0");

        // Create an SQLite file without any schema_version table
        let path = Path::new(&config.path);
        {
            let conn = Connection::open(path).expect("open raw");
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA foreign_keys = ON;
                 PRAGMA busy_timeout = 5000;
                 PRAGMA synchronous = NORMAL;",
            )
            .expect("pragmas");
        }

        // Now open via StateStore — should run migrations
        let store = StateStore::open(&config).expect("open store (migration)");

        // Verify tables exist
        let mut stmt = store
            .conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='offsets'")
            .unwrap();
        let _count: i64 = stmt.query_row([], |row| row.get::<_, i64>(0)).unwrap_or(0);
        // The table name as a scalar is actually a string, count rows instead
        drop(stmt);

        // Check that offsets table exists
        let tables_exist: bool = store
            .conn
            .prepare("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='offsets'")
            .and_then(|mut s| s.query_row([], |row| row.get::<_, i64>(0)))
            .map(|c| c > 0)
            .unwrap_or(false);
        assert!(tables_exist, "offsets table should exist after migration");

        cleanup(&config);
    }

    // ------------------------------------------------------------------
    // Test 13: Skipped LSNs
    // ------------------------------------------------------------------

    #[test]
    fn test_store_skipped_lsns() {
        let config = temp_config("skipped_positions");
        let store = StateStore::open(&config).expect("open store");

        store
            .record_skipped_lsn("0/DEADBEEF", "tx_err", "decoding failed")
            .expect("record skipped");

        let mut stmt = store
            .conn
            .prepare("SELECT position, tx_id, error_message FROM skipped_positions")
            .unwrap();
        let results: Vec<(String, String, String)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "0/DEADBEEF");
        assert_eq!(results[0].2, "decoding failed");

        cleanup(&config);
    }

    // ------------------------------------------------------------------
    // Test 14: Clear offsets
    // ------------------------------------------------------------------

    #[test]
    fn test_store_clear_offsets() {
        let config = temp_config("clear_offsets");
        let store = StateStore::open(&config).expect("open store");
        let lsn = Lsn::from_str("0/C0CAC01A").unwrap();
        store
            .write_offset(&lsn, "tx_clear", 5000, true, "pgoutput")
            .expect("write offset");
        assert!(store.read_last_offset().expect("read").is_some());

        store.clear_offsets().expect("clear offsets");
        assert!(store.read_last_offset().expect("read").is_none());

        cleanup(&config);
    }

    // ------------------------------------------------------------------
    // Test 15: Snapshot for non-existent table returns None
    // ------------------------------------------------------------------

    #[test]
    fn test_store_snapshot_status_nonexistent() {
        let config = temp_config("snap_nonexist");
        let store = StateStore::open(&config).expect("open store");

        let status = store
            .get_snapshot_status("public.nonexistent")
            .expect("get status");
        assert!(status.is_none());

        cleanup(&config);
    }

    // ------------------------------------------------------------------
    // Test 16: WAL mode concurrent reads
    // ------------------------------------------------------------------

    #[test]
    fn test_store_concurrent_access() {
        let config = temp_config("concurrent");

        // Open the store (holds exclusive lock on this connection)
        let store = StateStore::open(&config).expect("open store");

        // Write some offsets via the store
        for i in 0..3 {
            let lsn_hex = format!("0/{:08X}", 0x1000 + i);
            let lsn = Lsn::from_str(&lsn_hex).unwrap();
            store
                .write_offset(
                    &lsn,
                    &format!("tx_{i}"),
                    1000 + i as u64,
                    i == 2,
                    "pgoutput",
                )
                .expect("write offset");
        }

        // Open a second connection to the same DB file — WAL mode allows
        // concurrent readers even while the first connection holds a write
        // transaction.
        let conn2 = Connection::open(Path::new(&config.path)).expect("open second connection");
        let count: i64 = conn2
            .query_row("SELECT COUNT(*) FROM offsets", [], |row| row.get(0))
            .expect("count offsets from second connection");
        assert_eq!(count, 3, "concurrent reader should see all offsets");

        // Also verify we can read a specific value
        let lsn: String = conn2
            .query_row(
                "SELECT position FROM offsets ORDER BY sequence DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .expect("read latest position");
        assert_eq!(lsn, format!("0/{:08X}", 0x1002));

        drop(conn2);
        cleanup(&config);
    }

    // ------------------------------------------------------------------
    // Test 17: count_incomplete_chunks
    // ------------------------------------------------------------------

    #[test]
    fn test_store_count_incomplete_chunks() {
        let config = temp_config("incomplete_chunks");
        let store = StateStore::open(&config).expect("open store");

        // No chunks yet → 0 incomplete
        assert_eq!(
            store.count_incomplete_chunks("run_1").expect("count empty"),
            0
        );

        // Insert a completed chunk
        store
            .write_chunk("public.t", "run_1", 0, Some("1"), Some("10"))
            .expect("write chunk 0");
        store
            .complete_chunk("run_1", "public.t", 0, 9)
            .expect("complete chunk 0");

        // Still 0 incomplete
        assert_eq!(
            store
                .count_incomplete_chunks("run_1")
                .expect("count after complete"),
            0
        );

        // Insert a pending chunk (ON CONFLICT DO NOTHING, so different index)
        store
            .write_chunk("public.t", "run_1", 1, Some("10"), Some("20"))
            .expect("write chunk 1");

        // Now 1 incomplete
        assert_eq!(
            store
                .count_incomplete_chunks("run_1")
                .expect("count after pending"),
            1
        );

        // Different run_id → 0
        assert_eq!(
            store
                .count_incomplete_chunks("run_other")
                .expect("count other"),
            0
        );

        cleanup(&config);
    }
}
