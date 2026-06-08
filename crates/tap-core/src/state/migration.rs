//! Schema migration support for the SQLite state store.
//!
//! Tracks the current schema version in the `schema_version` table and
//! applies forward-only SQL migrations sequentially.  The initial migration
//! (version 1) creates all six tables used by [`StateStore`](super::store::StateStore),
//! and version 2 renames columns/table names and adds the `snapshot_chunks` table.

use rusqlite::Connection;
use tracing::info;

use crate::error::TapError;

/// The latest schema version understood by this build.
const LATEST_VERSION: i64 = 2;

/// Run all pending forward-only migrations on the given connection.
///
/// If the `schema_version` table does not exist it is created with an
/// implicit version of `0` (no migrations applied).  Each migration is
/// applied in order up to [`LATEST_VERSION`].
///
/// # Errors
///
/// Returns [`TapError::Sqlite`] on any database error during migration.
pub fn migrate(conn: &Connection) -> Result<(), TapError> {
    let current = current_version(conn)?;

    if current >= LATEST_VERSION {
        return Ok(());
    }

    for version in (current + 1)..=LATEST_VERSION {
        info!(version, "applying schema migration");
        apply_migration(conn, version)?;
        set_version(conn, version)?;
    }

    Ok(())
}

/// Read the current schema version from the `schema_version` table.
///
/// Returns `0` if the table does not exist (pre-migration state).
fn current_version(conn: &Connection) -> Result<i64, TapError> {
    // Check if schema_version exists
    let exists: bool = conn
        .prepare("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='schema_version'")?
        .query_row([], |row| row.get::<_, i64>(0))
        .map(|c| c > 0)?;

    if !exists {
        return Ok(0);
    }

    let version: i64 = conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_version",
        [],
        |row| row.get(0),
    )?;

    Ok(version)
}

/// Record the schema version after a successful migration.
fn set_version(conn: &Connection, version: i64) -> Result<(), TapError> {
    conn.execute(
        "INSERT INTO schema_version (version) VALUES (?1)",
        rusqlite::params![version],
    )?;
    Ok(())
}

/// Apply the SQL migration for `version`.
fn apply_migration(conn: &Connection, version: i64) -> Result<(), TapError> {
    match version {
        1 => {
            conn.execute_batch(MIGRATION_V1)?;
        }
        2 => {
            conn.execute_batch(MIGRATION_V2)?;
        }
        _ => {
            return Err(TapError::StateCorruption(format!(
                "unknown migration version: {version}"
            )));
        }
    }
    Ok(())
}

/// Version 1: create all six tables.
const MIGRATION_V1: &str = r#"
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS offsets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    committed_lsn TEXT NOT NULL UNIQUE,
    tx_id TEXT NOT NULL,
    ts_ms INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    is_final INTEGER NOT NULL DEFAULT 0,
    instance_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    rows_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'in_progress',
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at TEXT,
    error_message TEXT,
    snapshot_lsn TEXT NOT NULL,
    UNIQUE(table_name, snapshot_id)
);

CREATE TABLE IF NOT EXISTS schema_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL UNIQUE,
    columns_json TEXT NOT NULL,
    primary_keys TEXT NOT NULL DEFAULT '[]',
    relation_oid INTEGER,
    schema_version INTEGER NOT NULL DEFAULT 1,
    last_validated TEXT NOT NULL DEFAULT (datetime('now')),
    schema_hash TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS skipped_lsns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lsn TEXT NOT NULL,
    tx_id TEXT,
    error_message TEXT NOT NULL,
    raw_data TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS instance_info (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"#;

/// Version 2: v0.1.0 → v0.2.0 schema migration.
///
/// Changes:
/// - Rename `offsets.committed_lsn` → `offsets.position`
/// - Add `offsets.adapter` column (default `'pgoutput'`)
/// - Rename `skipped_lsns` → `skipped_positions`; rename `lsn` → `position`
/// - Create `snapshot_chunks` table for large-table chunking support
const MIGRATION_V2: &str = r#"
ALTER TABLE offsets RENAME COLUMN committed_lsn TO position;
ALTER TABLE offsets ADD COLUMN adapter TEXT NOT NULL DEFAULT 'pgoutput';
ALTER TABLE skipped_lsns RENAME TO skipped_positions;
ALTER TABLE skipped_positions RENAME COLUMN lsn TO position;
CREATE TABLE IF NOT EXISTS snapshot_chunks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    chunk_start TEXT,
    chunk_end TEXT,
    rows_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    error_message TEXT,
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at TEXT,
    UNIQUE(snapshot_id, table_name, chunk_index)
);
"#;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: creates a fresh in-memory SQLite connection for testing.
    fn memory_conn() -> Connection {
        let conn = Connection::open_in_memory().expect("open in-memory");
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;
             PRAGMA synchronous = NORMAL;",
        )
        .expect("set pragmas");
        conn
    }

    #[test]
    fn test_migration_empty_db_returns_version_0() {
        let conn = memory_conn();
        assert_eq!(current_version(&conn).unwrap(), 0);
    }

    #[test]
    fn test_migration_v1_creates_tables() {
        let conn = memory_conn();
        // Apply only v1 migration (not the full chain to v2)
        apply_migration(&conn, 1).expect("apply v1");
        set_version(&conn, 1).expect("set version");

        // Check version
        assert_eq!(current_version(&conn).unwrap(), 1);

        // Verify all 6 v1 tables exist
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        let expected = [
            "instance_info",
            "offsets",
            "schema_cache",
            "schema_version",
            "skipped_lsns",
            "snapshots",
        ];

        for name in &expected {
            assert!(tables.contains(&name.to_string()), "missing table: {name}");
        }
    }

    #[test]
    fn test_migration_idempotent() {
        let conn = memory_conn();
        migrate(&conn).expect("first migration");
        migrate(&conn).expect("second migration (idempotent)");

        assert_eq!(current_version(&conn).unwrap(), 2);
    }

    #[test]
    fn test_migration_offsets_table_structure() {
        let conn = memory_conn();
        migrate(&conn).expect("migrate");

        // Verify offsets columns exist (v2 schema: position instead of committed_lsn)
        let cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('offsets') ORDER BY cid")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert!(cols.contains(&"position".into()), "missing position column");
        assert!(cols.contains(&"adapter".into()), "missing adapter column");
        assert!(cols.contains(&"tx_id".into()));
        assert!(cols.contains(&"ts_ms".into()));
        assert!(cols.contains(&"sequence".into()));
        assert!(cols.contains(&"is_final".into()));
    }

    #[test]
    fn test_current_version_0_no_table() {
        let conn = memory_conn();
        // Without schema_version table, current_version returns 0
        assert_eq!(current_version(&conn).unwrap(), 0);
    }

    #[test]
    fn test_migration_v2_applies_on_top_of_v1() {
        let conn = memory_conn();

        // 1. Apply only v1 migration (not the full chain to v2)
        apply_migration(&conn, 1).expect("apply v1");
        set_version(&conn, 1).expect("set version");
        assert_eq!(current_version(&conn).unwrap(), 1);

        // 2. Insert a row into offsets using v1 schema
        conn.execute(
            "INSERT INTO offsets (committed_lsn, tx_id, ts_ms, sequence, is_final)
             VALUES ('0/DEADBEEF', 'tx_v1', 1000, 1, 1)",
            [],
        )
        .expect("insert v1 offset");

        // 3. Run migration to v2 (migrate sees version=1, applies v2)
        migrate(&conn).expect("migrate to v2");
        assert_eq!(current_version(&conn).unwrap(), 2);

        // 5. Verify position column exists and carries the old committed_lsn value
        let position: String = conn
            .query_row(
                "SELECT position FROM offsets WHERE sequence = 1",
                [],
                |row| row.get(0),
            )
            .expect("read position");
        assert_eq!(
            position, "0/DEADBEEF",
            "position should carry old committed_lsn value"
        );

        // 6. Verify adapter column exists with default
        let adapter: String = conn
            .query_row(
                "SELECT adapter FROM offsets WHERE sequence = 1",
                [],
                |row| row.get(0),
            )
            .expect("read adapter");
        assert_eq!(adapter, "pgoutput", "adapter should have default value");

        // 7. Verify skipped_positions table exists
        let table_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='skipped_positions'",
                [],
                |row| row.get(0),
            )
            .expect("check table");
        assert_eq!(table_count, 1, "skipped_positions table should exist");

        // 8. Verify snapshot_chunks table exists
        let chunk_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='snapshot_chunks'",
                [],
                |row| row.get(0),
            )
            .expect("check table");
        assert_eq!(chunk_count, 1, "snapshot_chunks table should exist");

        // 9. Verify old skipped_lsns table is gone
        let old_table_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='skipped_lsns'",
                [],
                |row| row.get(0),
            )
            .expect("check table");
        assert_eq!(old_table_count, 0, "skipped_lsns table should be gone");
    }
}
