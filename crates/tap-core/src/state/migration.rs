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
pub fn migrate(conn: &mut Connection) -> Result<(), TapError> {
    let current = current_version(conn)?;

    if current >= LATEST_VERSION {
        return Ok(());
    }

    for version in (current + 1)..=LATEST_VERSION {
        info!(version, "applying schema migration");
        // apply_migration wraps DDL + version bump in a single transaction,
        // so no separate set_version call is needed here.
        apply_migration(conn, version)?;
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

/// Apply the SQL migration for `version` atomically (DDL + version bump
/// in a single transaction).  If the process dies mid-migration the entire
/// batch is rolled back, preventing a crash-loop on restart.
fn apply_migration(conn: &mut Connection, version: i64) -> Result<(), TapError> {
    let tx = conn.transaction()?;
    match version {
        1 => {
            tx.execute_batch(MIGRATION_V1)?;
        }
        2 => {
            tx.execute_batch(MIGRATION_V2)?;
        }
        _ => {
            return Err(TapError::StateCorruption(format!(
                "unknown migration version: {version}"
            )));
        }
    }
    // Write version inside the same transaction so the schema version
    // always stays in sync with the applied DDL.
    tx.execute(
        "INSERT OR IGNORE INTO schema_version (version) VALUES (?1)",
        rusqlite::params![version],
    )?;
    tx.commit()?;
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
-- Rebuild offsets table WITHOUT the UNIQUE constraint on position.
-- The v1 DDL had `committed_lsn TEXT NOT NULL UNIQUE`, and ALTER TABLE
-- RENAME COLUMN preserves the index.  Without this rebuild we cannot
-- write the same LSN twice (e.g. is_final=0 during streaming and
-- is_final=1 during shutdown).
CREATE TABLE IF NOT EXISTS offsets_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    position TEXT NOT NULL,
    tx_id TEXT NOT NULL,
    ts_ms INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    is_final INTEGER NOT NULL DEFAULT 0,
    adapter TEXT NOT NULL DEFAULT 'pgoutput',
    instance_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
INSERT INTO offsets_new (id, position, tx_id, ts_ms, sequence, is_final, adapter, instance_id, created_at)
SELECT id, committed_lsn, tx_id, ts_ms, sequence, is_final, 'pgoutput', instance_id, created_at FROM offsets;
DROP TABLE offsets;
ALTER TABLE offsets_new RENAME TO offsets;
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
        let mut conn = memory_conn();
        assert_eq!(current_version(&conn).unwrap(), 0);
    }

    #[test]
    fn test_migration_v1_creates_tables() {
        let mut conn = memory_conn();
        // Apply only v1 migration (not the full chain to v2).
        // apply_migration now writes the version inside the same transaction.
        apply_migration(&mut conn, 1).expect("apply v1");

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
        let mut conn = memory_conn();
        migrate(&mut conn).expect("first migration");
        migrate(&mut conn).expect("second migration (idempotent)");

        assert_eq!(current_version(&conn).unwrap(), 2);
    }

    #[test]
    fn test_migration_offsets_table_structure() {
        let mut conn = memory_conn();
        migrate(&mut conn).expect("migrate");

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

        // Verify UNIQUE constraint on position has been removed.
        let uniques: Vec<String> = conn
            .prepare("SELECT il.name FROM pragma_index_list('offsets') il WHERE il.origin = 'u'")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert!(
            !uniques.contains(&"sqlite_autoindex_offsets_1".into()),
            "UNIQUE on position should be removed"
        );
    }

    #[test]
    fn test_write_same_lsn_twice() {
        let mut conn = memory_conn();
        migrate(&mut conn).expect("migrate");

        // Write the same LSN as non-final then final — must not error.
        conn.execute(
            "INSERT INTO offsets (position, tx_id, ts_ms, sequence, is_final, adapter)
             VALUES (?1, 'tx1', 1000, 1, 0, 'pgoutput')",
            rusqlite::params!["0/DEADBEEF"],
        )
        .expect("first insert (non-final)");

        conn.execute(
            "INSERT INTO offsets (position, tx_id, ts_ms, sequence, is_final, adapter)
             VALUES (?1, '0', 0, 2, 1, 'pgoutput')",
            rusqlite::params!["0/DEADBEEF"],
        )
        .expect("second insert (final, same LSN)");

        // Both rows should exist.
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM offsets WHERE position = '0/DEADBEEF'",
                [],
                |row| row.get(0),
            )
            .expect("count rows");
        assert_eq!(count, 2, "should have two rows with same position");
    }

    #[test]
    fn test_current_version_0_no_table() {
        let mut conn = memory_conn();
        // Without schema_version table, current_version returns 0
        assert_eq!(current_version(&conn).unwrap(), 0);
    }

    #[test]
    fn test_migration_v2_applies_on_top_of_v1() {
        let mut conn = memory_conn();

        // 1. Apply only v1 migration (not the full chain to v2).
        //    apply_migration now writes the version inside the same transaction.
        apply_migration(&mut conn, 1).expect("apply v1");
        assert_eq!(current_version(&conn).unwrap(), 1);

        // 2. Insert a row into offsets using v1 schema
        conn.execute(
            "INSERT INTO offsets (committed_lsn, tx_id, ts_ms, sequence, is_final)
             VALUES ('0/DEADBEEF', 'tx_v1', 1000, 1, 1)",
            [],
        )
        .expect("insert v1 offset");

        // 3. Run migration to v2 (migrate sees version=1, applies v2)
        migrate(&mut conn).expect("migrate to v2");
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
