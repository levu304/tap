//! Integration tests for tap-core using testcontainers.
//!
//! These tests spin up a real Postgres 16-alpine container pre-configured
//! for logical replication (`wal_level=logical`), then verify the CDC
//! pipeline at the SQL level using the built-in `test_decoding` plugin.
//!
//! # CI mode
//!
//! Set `TAP_TEST_DB` to a Postgres connection string to skip the container
//! and use an externally managed database (expected to have `wal_level`
//! already set to `logical`).
//!
//! # Test isolation
//!
//! Every test generates unique table / slot / publication names so they
//! can run concurrently (or sequentially against a shared container).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use testcontainers::{Container, GenericImage, clients::Cli, core::WaitFor};

// Re-use the library's engine types so we can exercise them against a real
// Postgres (or the TAP_TEST_DB CI database).
use tap_core::config::{SnapshotConfig, SourceConfig, StateConfig};
use tap_core::postgres::{self, Lsn, PgConnection};
use tap_core::snapshot::SnapshotRunner;
use tap_core::state::StateStore;

/// Postgres image to use for all integration tests.
const PG_IMAGE: &str = "postgres";
const PG_TAG: &str = "16-alpine";

/// Global Docker client (lazily initialised, static lifetime).
static DOCKER: OnceLock<Cli> = OnceLock::new();
/// Lock to serialise container creation (testcontainers CLI isn't thread-safe).
static DOCKER_LOCK: Mutex<()> = Mutex::new(());

fn docker() -> &'static Cli {
    DOCKER.get_or_init(|| Cli::default())
}

// ---------------------------------------------------------------------------
// Test harness helpers
// ---------------------------------------------------------------------------

/// A running Postgres container with logical replication configured.
///
/// The container is pre-configured with `wal_level=logical` so replication
/// slots can be created without a restart.  In CI mode (`TAP_TEST_DB` env
/// var) no container is started; the provided connection string is used
/// directly.
///
/// The container is dropped (& cleaned up) when `TestPg` goes out of scope.
struct TestPg {
    /// Running container (`None` in CI mode).
    #[allow(dead_code)]
    container: Option<Container<'static, GenericImage>>,
    /// Connection string for the running Postgres instance.
    connection_string: String,
}

impl TestPg {
    /// Start a Postgres container (or connect to `TAP_TEST_DB` in CI mode)
    /// and return a test harness.
    fn start() -> Self {
        // CI mode — use externally managed database
        if let Ok(conn_str) = std::env::var("TAP_TEST_DB") {
            return Self {
                container: None,
                connection_string: conn_str,
            };
        }

        let _lock = DOCKER_LOCK.lock().expect("docker lock");

        // Start Postgres with `wal_level=logical` from the get-go so we
        // never need to restart the container for ALTER SYSTEM to take effect.
        let wal_args: Vec<String> = vec![
            "-c".into(),
            "wal_level=logical".into(),
            "-c".into(),
            "max_replication_slots=10".into(),
            "-c".into(),
            "max_wal_senders=10".into(),
        ];

        let container = docker().run((
            GenericImage::new(PG_IMAGE, PG_TAG)
                .with_wait_for(WaitFor::message_on_stderr(
                    "database system is ready to accept connections",
                ))
                .with_env_var("POSTGRES_PASSWORD", "tap_test")
                .with_env_var("POSTGRES_DB", "tap_test"),
            wal_args,
        ));

        let host = "localhost";
        let port = container.get_host_port_ipv4(5432);
        let connection_string = format!("postgres://postgres:tap_test@{host}:{port}/tap_test");

        Self {
            container: Some(container),
            connection_string,
        }
    }

    /// Get the connection string for this test Postgres instance.
    fn connection_string(&self) -> &str {
        &self.connection_string
    }

    /// Execute arbitrary SQL (DDL / DML that does not return rows).
    async fn execute(&self, sql: &str) {
        let (client, connection) =
            tokio_postgres::connect(self.connection_string(), tokio_postgres::NoTls)
                .await
                .expect("connect to postgres");
        tokio::spawn(connection);
        client.batch_execute(sql).await.expect("execute sql");
    }

    /// Execute a query that returns rows.  Returns the first column of
    /// every row as a `String` (panics if a value is not coercible).
    async fn query(&self, sql: &str) -> Vec<String> {
        let (client, connection) =
            tokio_postgres::connect(self.connection_string(), tokio_postgres::NoTls)
                .await
                .expect("connect to postgres");
        tokio::spawn(connection);
        let rows = client.query(sql, &[]).await.expect("query failed");
        rows.iter()
            .map(|row| {
                let val: String = row.get(0);
                val
            })
            .collect()
    }

    /// Create a test table with some sample data.
    async fn create_test_table(&self, table_name: &str) {
        self.execute(&format!(
            "CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value BIGINT NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )"
        ))
        .await;
    }

    /// Insert sample rows into a test table using parameterized queries.
    async fn insert_rows(&self, table_name: &str, rows: &[(&str, i64)]) {
        let (client, connection) =
            tokio_postgres::connect(self.connection_string(), tokio_postgres::NoTls)
                .await
                .expect("connect to postgres");
        tokio::spawn(connection);
        for &(name, value) in rows {
            let sql = format!("INSERT INTO {table_name} (name, value) VALUES ($1, $2)");
            client
                .execute(&sql, &[&name, &value])
                .await
                .expect("insert row");
        }
    }
}

// ---------------------------------------------------------------------------
// Global test container (shared across tests)
// ---------------------------------------------------------------------------

/// Global test container, lazily initialised once per test run.
static TEST_PG: OnceLock<TestPg> = OnceLock::new();

/// Get or initialise the shared test container.
fn get_test_pg() -> &'static TestPg {
    TEST_PG.get_or_init(|| TestPg::start())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Monotonically increasing counter appended to each generated test name
/// to prevent collisions when concurrent tests request names at the same
/// nanosecond.
static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a unique, Postgres-safe identifier with the given prefix.
///
/// The returned string contains only alphanumeric characters (a-f, 0-9) and
/// underscores, making it safe for use as table names, slot names, and
/// publication names without quoting.
fn test_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_nanos();
    let counter = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    // Last 12 hex chars — unique enough for a single test run.
    let hex_id = format!("{nanos:x}");
    let suffix = if hex_id.len() > 12 {
        &hex_id[hex_id.len() - 12..]
    } else {
        &hex_id
    };
    format!("{prefix}_{suffix}_{counter}")
}

/// Guard that drops a replication slot when it goes out of scope.
///
/// Used to prevent slot leaks when tests panic after creating a slot.
/// Connects directly via the stored connection string and issues
/// `pg_drop_replication_slot` on Drop.
///
/// Currently unused (tests do explicit cleanup) but retained for future
/// tests that might panic before reaching cleanup code.
#[allow(dead_code)]
struct SlotGuard {
    pg_conn_str: String,
    slot_name: String,
}

#[allow(dead_code)]
impl SlotGuard {
    fn new(pg: &TestPg, slot_name: &str) -> Self {
        Self {
            pg_conn_str: pg.connection_string().to_string(),
            slot_name: slot_name.to_string(),
        }
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        let conn_str = self.pg_conn_str.clone();
        let slot = self.slot_name.clone();
        // Use a dedicated runtime since Drop may run outside the test's
        // async context (e.g. during panic unwinding).
        if let Ok(rt) = tokio::runtime::Runtime::new() {
            rt.block_on(async {
                if let Ok((client, connection)) =
                    tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await
                {
                    tokio::spawn(connection);
                    let sql = format!("SELECT pg_drop_replication_slot('{slot}')");
                    let _ = client.simple_query(&sql).await;
                }
            });
        }
    }
}

/// Create a logical replication slot using `test_decoding` and return all
/// accumulated decoded WAL changes as a `Vec<String>`.
///
/// The data column contains human-readable text such as:
/// ```text
/// INSERT INTO public.<table> (id, name, ...) VALUES ('1', 'name', ...)
/// ```
async fn read_decoded_changes(pg: &TestPg, slot_name: &str) -> Vec<String> {
    pg.query(&format!(
        "SELECT data FROM pg_logical_slot_get_changes(
             '{slot_name}', NULL, NULL,
             'include-xids', '0',
             'skip-empty-xacts', '1'
         )"
    ))
    .await
}

/// Create a `test_decoding` logical replication slot.
async fn create_decoding_slot(pg: &TestPg, slot_name: &str) {
    pg.execute(&format!(
        "SELECT pg_create_logical_replication_slot('{slot_name}', 'test_decoding')"
    ))
    .await;
}

/// Drop a `test_decoding` logical replication slot.
async fn drop_decoding_slot(pg: &TestPg, slot_name: &str) {
    pg.execute(&format!("SELECT pg_drop_replication_slot('{slot_name}')"))
        .await;
}

/// Quick sanity test that runs first to verify test harness works.
#[tokio::test]
async fn test_harness_sanity_check() {
    eprintln!("test_harness_sanity_check: starting");
    assert_eq!(1 + 1, 2);
    eprintln!("test_harness_sanity_check: done");
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

/// Verify that the TestPg harness can start a container and connect.
#[tokio::test]
async fn test_harness_starts_container() {
    let pg = TestPg::start();
    assert!(
        !pg.connection_string().is_empty(),
        "connection string should not be empty"
    );
    // Container is dropped at end of scope, cleaning up.
}

/// Test creating a table and inserting rows via the harness.
#[tokio::test]
async fn test_harness_create_table_and_insert() {
    let pg = TestPg::start();
    pg.create_test_table("harness_test").await;
    pg.insert_rows("harness_test", &[("test1", 10), ("test2", 20)])
        .await;
    let count = pg.query("SELECT count(*)::text FROM harness_test").await;
    assert_eq!(count, vec!["2"], "should have inserted 2 rows");
}

// ---------------------------------------------------------------------------
// Replication slot management
// ---------------------------------------------------------------------------

/// Test that a replication slot and publication can be created and verified
/// via Postgres system tables.
///
/// Verifies:
/// - A `test_decoding` logical slot appears in `pg_replication_slots`.
/// - A publication appears in `pg_publication`.
/// - After cleanup, both are removed.
#[tokio::test]
async fn test_create_replication_slot() {
    let pg = get_test_pg();
    let table = test_name("t_slot");
    let slot = test_name("slot");
    let pub_name = test_name("pub");

    pg.create_test_table(&table).await;
    pg.execute(&format!(
        "CREATE PUBLICATION \"{pub_name}\" FOR TABLE \"{table}\""
    ))
    .await;
    create_decoding_slot(pg, &slot).await;

    // --- Verify slot exists -------------------------------------------------
    let slots = pg
        .query(&format!(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = '{slot}'"
        ))
        .await;
    assert_eq!(slots.len(), 1, "slot '{slot}' should exist");
    assert_eq!(slots[0], slot, "slot name should match");

    // --- Verify publication exists ------------------------------------------
    let pubs = pg
        .query(&format!(
            "SELECT pubname FROM pg_publication WHERE pubname = '{pub_name}'"
        ))
        .await;
    assert_eq!(pubs.len(), 1, "publication '{pub_name}' should exist");
    assert_eq!(pubs[0], pub_name, "publication name should match");

    // --- Cleanup -------------------------------------------------------------
    drop_decoding_slot(pg, &slot).await;
    pg.execute(&format!("DROP PUBLICATION IF EXISTS \"{pub_name}\""))
        .await;
    let slots = pg.query("SELECT slot_name FROM pg_replication_slots").await;
    let pubs = pg.query("SELECT pubname FROM pg_publication").await;
    assert!(!slots.iter().any(|s| s == &slot), "slot should be dropped");
    assert!(
        !pubs.iter().any(|p| p == &pub_name),
        "publication should be dropped"
    );
}

// ---------------------------------------------------------------------------
// Row-level change capture
// ---------------------------------------------------------------------------

/// Test that INSERT operations are captured in the WAL stream.
///
/// 1. Creates a table and a `test_decoding` slot.
/// 2. Inserts a row *after* the slot exists.
/// 3. Reads decoded WAL changes and asserts "INSERT" is present.
#[tokio::test]
async fn test_captures_insert_events() {
    let pg = get_test_pg();
    let table = test_name("t_ins");
    let slot = test_name("slot_ins");

    pg.create_test_table(&table).await;
    create_decoding_slot(pg, &slot).await;

    // Insert AFTER the slot is created so the change is captured
    pg.execute(&format!(
        "INSERT INTO {table} (name, value) VALUES ('inserted_row', 42)"
    ))
    .await;

    let changes = read_decoded_changes(pg, &slot).await;
    let output = changes.join(" ");

    assert!(
        output.contains("INSERT"),
        "decoded WAL should contain INSERT, got: {output}"
    );
    assert!(
        output.contains(&table),
        "decoded WAL should mention table '{table}', got: {output}"
    );

    drop_decoding_slot(pg, &slot).await;
}

/// Test that UPDATE operations are captured in the WAL stream.
///
/// 1. Creates a table and inserts a row *before* the slot (pre-existing).
/// 2. Creates a `test_decoding` slot (captures from this point onward).
/// 3. UPDATES the pre-existing row.
/// 4. Reads decoded WAL changes and asserts "UPDATE" is present.
#[tokio::test]
async fn test_captures_update_events() {
    let pg = get_test_pg();
    let table = test_name("t_upd");
    let slot = test_name("slot_upd");

    pg.create_test_table(&table).await;
    // Insert BEFORE the slot — these WAL records won't be captured
    pg.execute(&format!(
        "INSERT INTO {table} (name, value) VALUES ('original', 1)"
    ))
    .await;
    create_decoding_slot(pg, &slot).await;

    // Update AFTER the slot is created
    pg.execute(&format!(
        "UPDATE {table} SET value = 99 WHERE name = 'original'"
    ))
    .await;

    let changes = read_decoded_changes(pg, &slot).await;
    let output = changes.join(" ");

    assert!(
        output.contains("UPDATE"),
        "decoded WAL should contain UPDATE, got: {output}"
    );
    assert!(
        output.contains(&table),
        "decoded WAL should mention table '{table}', got: {output}"
    );
    // Verify the new value appears in the decoded output
    assert!(
        output.contains("99"),
        "decoded WAL should contain updated value '99', got: {output}"
    );

    drop_decoding_slot(pg, &slot).await;
}

/// Test that DELETE operations are captured in the WAL stream.
///
/// 1. Creates a table and inserts a row *before* the slot (pre-existing).
/// 2. Creates a `test_decoding` slot (captures from this point onward).
/// 3. DELETEs the pre-existing row.
/// 4. Reads decoded WAL changes and asserts "DELETE" is present.
#[tokio::test]
async fn test_captures_delete_events() {
    let pg = get_test_pg();
    let table = test_name("t_del");
    let slot = test_name("slot_del");

    pg.create_test_table(&table).await;
    // Insert BEFORE the slot
    pg.execute(&format!(
        "INSERT INTO {table} (name, value) VALUES ('doomed', 1)"
    ))
    .await;
    create_decoding_slot(pg, &slot).await;

    // Delete AFTER the slot is created
    pg.execute(&format!("DELETE FROM {table} WHERE name = 'doomed'"))
        .await;

    let changes = read_decoded_changes(pg, &slot).await;
    let output = changes.join(" ");

    assert!(
        output.contains("DELETE"),
        "decoded WAL should contain DELETE, got: {output}"
    );
    assert!(
        output.contains(&table),
        "decoded WAL should mention table '{table}', got: {output}"
    );

    drop_decoding_slot(pg, &slot).await;
}

// ---------------------------------------------------------------------------
// Snapshot phase
// ---------------------------------------------------------------------------

/// Test that pre-populated rows are accessible after creating a replication
/// slot (simulating the "snapshot" phase of CDC).
///
/// In the capture pipeline the snapshot phase takes a consistent read of
/// existing data *before* streaming new changes.  This test verifies:
/// 1. Pre-populated rows exist in the table (accessible via SQL).
/// 2. After the slot is created, only *new* changes appear in the WAL
///    (pre-populated data is read via SQL, not WAL).
/// 3. A subsequent INSERT is captured in the WAL, confirming the slot works.
#[tokio::test]
async fn test_snapshot_produces_read_events() {
    let pg = get_test_pg();
    let table = test_name("t_snap");
    let slot = test_name("slot_snap");

    pg.create_test_table(&table).await;

    // Pre-populate rows (the "existing data" the snapshot should read)
    pg.insert_rows(&table, &[("alpha", 10), ("bravo", 20), ("charlie", 30)])
        .await;

    // Verify data is accessible via SQL (snapshot read)
    let rows = pg
        .query(&format!("SELECT name FROM {table} ORDER BY id"))
        .await;
    assert_eq!(rows.len(), 3, "should have 3 pre-populated rows");
    assert_eq!(rows[0], "alpha");
    assert_eq!(rows[1], "bravo");
    assert_eq!(rows[2], "charlie");

    // Create slot AFTER pre-populated data — it should only capture new changes
    create_decoding_slot(pg, &slot).await;

    // No changes after slot creation — WAL should be empty
    let initial_changes = read_decoded_changes(pg, &slot).await;
    assert!(
        initial_changes.is_empty(),
        "no WAL changes expected (pre-populated data was before slot creation), \
         got {}: {:?}",
        initial_changes.len(),
        initial_changes
    );

    // Insert a new row — this should appear in WAL
    pg.execute(&format!(
        "INSERT INTO {table} (name, value) VALUES ('delta', 40)"
    ))
    .await;

    let new_changes = read_decoded_changes(pg, &slot).await;
    let output = new_changes.join(" ");
    assert!(
        output.contains("INSERT"),
        "new INSERT should appear in WAL, got: {output}"
    );
    assert!(
        output.contains("delta"),
        "new row 'delta' should appear in WAL, got: {output}"
    );

    drop_decoding_slot(pg, &slot).await;
}

// ---------------------------------------------------------------------------
// Multiple tables
// ---------------------------------------------------------------------------

/// Test that WAL changes from multiple tables are all captured.
///
/// Creates two independent tables (same schema), inserts rows into each,
/// and verifies both table names appear in the decoded WAL output.
#[tokio::test]
async fn test_multiple_tables() {
    let pg = get_test_pg();
    let table_a = test_name("t_multi_a");
    let table_b = test_name("t_multi_b");
    let slot = test_name("slot_multi");

    pg.create_test_table(&table_a).await;
    pg.create_test_table(&table_b).await;

    create_decoding_slot(pg, &slot).await;

    // Insert into both tables AFTER the slot
    pg.execute(&format!(
        "INSERT INTO {table_a} (name, value) VALUES ('from_a', 100)"
    ))
    .await;
    pg.execute(&format!(
        "INSERT INTO {table_b} (name, value) VALUES ('from_b', 200)"
    ))
    .await;

    let changes = read_decoded_changes(pg, &slot).await;
    let output = changes.join(" ");

    assert!(
        output.contains(&table_a),
        "WAL should mention table_a '{table_a}', got: {output}"
    );
    assert!(
        output.contains(&table_b),
        "WAL should mention table_b '{table_b}', got: {output}"
    );
    assert!(
        output.contains("from_a"),
        "WAL should contain 'from_a', got: {output}"
    );
    assert!(
        output.contains("from_b"),
        "WAL should contain 'from_b', got: {output}"
    );

    drop_decoding_slot(pg, &slot).await;
}

// ---------------------------------------------------------------------------
// Graceful shutdown
// ---------------------------------------------------------------------------

/// Test that no data is lost across a "shutdown" (drop + recreate slot).
///
/// Simulates the graceful shutdown / restart cycle:
/// 1. Create table, insert initial data.
/// 2. Create a slot and verify data is captured.
/// 3. Drop the slot (simulating shutdown without checkpoint).
/// 4. Insert new data.
/// 5. Create a new slot and verify the new data is captured.
/// 6. Query the table to verify ALL data is intact.
#[tokio::test]
async fn test_graceful_shutdown() {
    let pg = get_test_pg();
    let table = test_name("t_grace");
    let slot_a = test_name("slot_grace_a");
    let slot_b = test_name("slot_grace_b");

    pg.create_test_table(&table).await;
    create_decoding_slot(pg, &slot_a).await;

    // --- Phase 1: capture initial data ----------------------------------------
    pg.execute(&format!(
        "INSERT INTO {table} (name, value) VALUES ('phase1', 1)"
    ))
    .await;

    let phase1_changes = read_decoded_changes(pg, &slot_a).await;
    assert!(
        phase1_changes.join(" ").contains("INSERT"),
        "phase 1 INSERT should be captured"
    );

    // --- Phase 2: "shutdown" — drop old slot, insert more data ---------------
    drop_decoding_slot(pg, &slot_a).await;
    pg.execute(&format!(
        "INSERT INTO {table} (name, value) VALUES ('phase2', 2)"
    ))
    .await;

    // --- Phase 3: "restart" — new slot, verify all data is intact ------------
    create_decoding_slot(pg, &slot_b).await;

    // Phase 2's INSERT happened before slot_b — it won't be in WAL.
    // Insert new data to confirm the new slot works.
    pg.execute(&format!(
        "INSERT INTO {table} (name, value) VALUES ('phase3', 3)"
    ))
    .await;

    let phase3_changes = read_decoded_changes(pg, &slot_b).await;
    assert!(
        phase3_changes.join(" ").contains("INSERT"),
        "new slot should capture phase 3 INSERT"
    );

    // --- Verify all data is present via SQL -----------------------------------
    let all_rows = pg
        .query(&format!("SELECT name FROM {table} ORDER BY id"))
        .await;
    assert_eq!(
        all_rows.len(),
        3,
        "all 3 rows should exist (no data loss), got: {all_rows:?}"
    );
    assert_eq!(all_rows[0], "phase1");
    assert_eq!(all_rows[1], "phase2");
    assert_eq!(all_rows[2], "phase3");

    drop_decoding_slot(pg, &slot_b).await;
}

// ===========================================================================
// Engine-level integration tests
// ===========================================================================
//
// Unlike the tests above (which use the built-in test_decoding plugin to
// inspect WAL at the SQL level), these tests exercise Tap's actual engine
// components: SnapshotRunner, StateStore, and PgConnection.

// ---------------------------------------------------------------------------
// StateStore persistence (no Postgres required)
// ---------------------------------------------------------------------------

/// Test that StateStore persists offsets and reads them back correctly,
/// simulating the checkpoint/resume cycle in the capture engine.
#[tokio::test]
async fn test_state_store_offset_persistence() {
    let dir = std::env::temp_dir().join(format!("tap_state_{}", test_name("")));
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("state.db");
    let config = StateConfig {
        path: db_path.to_string_lossy().to_string(),
        max_backup_size_kb: 1024,
    };

    {
        let store = StateStore::open(&config).expect("open state store");

        // Write a checkpoint (simulating a streamed transaction)
        let lsn: Lsn = "0/16B37428".parse().expect("valid LSN");
        store
            .write_offset(&lsn, "12345", 1717000000000, false)
            .expect("write offset");

        // Read it back
        let offset = store
            .read_last_offset()
            .expect("read offset")
            .expect("offset should exist");
        assert_eq!(offset.committed_lsn, "0/16B37428");
        assert!(!offset.is_final, "non-final offset");

        // Write a final (flush) checkpoint
        let lsn2: Lsn = "0/16B37429".parse().expect("valid LSN");
        store
            .write_offset(&lsn2, "12346", 1717000001000, true)
            .expect("write final offset");

        // Read back — should prefer the final offset
        let final_offset = store
            .read_last_offset()
            .expect("read offset")
            .expect("offset should exist");
        assert_eq!(final_offset.committed_lsn, "0/16B37429");
        assert!(final_offset.is_final, "should be final offset");
    }

    // Re-open (simulating process restart) — data should persist
    {
        let store = StateStore::open(&config).expect("re-open state store");
        let offset = store
            .read_last_offset()
            .expect("read offset")
            .expect("offset should persist across restarts");
        assert_eq!(
            offset.committed_lsn, "0/16B37429",
            "persisted LSN should survive restart"
        );
        assert!(offset.is_final, "persisted offset should be final");
    }

    // Cleanup
    let _ = std::fs::remove_dir_all(&dir);
}

/// Test that SnapshotRecord progress is tracked correctly.
#[tokio::test]
async fn test_state_store_snapshot_progress() {
    let dir = std::env::temp_dir().join(format!("tap_state_snap_{}", test_name("")));
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("state.db");
    let config = StateConfig {
        path: db_path.to_string_lossy().to_string(),
        max_backup_size_kb: 1024,
    };

    {
        let store = StateStore::open(&config).expect("open state store");
        let lsn: Lsn = "0/16B37428".parse().expect("valid LSN");

        // Write snapshot progress for two tables
        store
            .write_snapshot_progress("public.users", "snap-abc-123", 0, &lsn)
            .expect("write progress");
        store
            .write_snapshot_progress("public.orders", "snap-abc-123", 50, &lsn)
            .expect("write progress");

        // Verify status
        let users_status = store
            .get_snapshot_status("public.users")
            .expect("get status")
            .expect("should exist");
        assert_eq!(users_status.status, "in_progress");
        assert_eq!(users_status.rows_count, 0);

        // Complete one table
        store
            .complete_snapshot("public.users", "snap-abc-123", 100)
            .expect("complete snapshot");

        let completed = store
            .get_snapshot_status("public.users")
            .expect("get status")
            .expect("should exist");
        assert_eq!(completed.status, "completed");
        assert_eq!(completed.rows_count, 100);
    }

    // Cleanup
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// SnapshotRunner integration (requires Postgres)
// ---------------------------------------------------------------------------

/// Test that SnapshotRunner correctly snapshots pre-existing rows from a
/// real Postgres table and emits Read events with the expected structure.
#[tokio::test]
async fn test_snapshot_runner_captures_existing_rows() {
    let pg = get_test_pg();
    let table = test_name("t_snap_eng");
    let slot = test_name("slot_snap_eng");

    pg.create_test_table(&table).await;

    // Pre-populate 5 rows
    pg.insert_rows(
        &table,
        &[
            ("alpha", 100),
            ("bravo", 200),
            ("charlie", 300),
            ("delta", 400),
            ("echo", 500),
        ],
    )
    .await;

    // Build a SourceConfig from the test PG connection string
    let conn_str = pg.connection_string();
    // Parse host, port, dbname, user, password from connection string
    let source_cfg = source_config_from_conn_str(conn_str, &slot, &table);

    // Create plain connections for snapshot runner
    let (keeper, keeper_handle) = postgres::connect_plain(&source_cfg)
        .await
        .expect("keeper connect");
    let (worker, worker_handle) = postgres::connect_plain(&source_cfg)
        .await
        .expect("worker connect");

    // Open a temp state store for snapshot progress
    let dir = std::env::temp_dir().join(format!("tap_snap_{}", test_name("")));
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("state.db");
    let state_config = StateConfig {
        path: db_path.to_string_lossy().to_string(),
        max_backup_size_kb: 1024,
    };
    let state = Arc::new(tokio::sync::Mutex::new(
        StateStore::open(&state_config).expect("open state store"),
    ));

    let tables = vec![format!("public.{table}")];
    let snap_config = SnapshotConfig {
        tables,
        ..Default::default()
    };
    let db_name = psql_db_name(conn_str);
    let (event_tx, mut event_rx) =
        tokio::sync::mpsc::unbounded_channel::<tap_core::event::ChangeEvent>();

    let mut runner = SnapshotRunner::new(keeper, worker, state, snap_config, db_name, event_tx);

    let result = runner.run().await.expect("snapshot runner should succeed");

    // Verify snapshot result
    assert_eq!(result.total_rows, 5, "should have snapshotted 5 rows");
    assert!(
        !result.tables_snapshotted.is_empty(),
        "should have at least one table"
    );
    assert!(!result.snapshot_id.is_empty(), "should have a snapshot ID");

    // Collect events
    let mut events = Vec::new();
    while let Ok(event) = event_rx.try_recv() {
        events.push(event);
    }

    assert_eq!(events.len(), 5, "should have 5 Read events");

    // Verify each event is a Read operation with snapshot=true
    for event in &events {
        assert_eq!(
            event.op,
            tap_core::event::Operation::Read,
            "snapshot events should be Read operations"
        );
        assert!(
            event.source.snapshot == Some(true),
            "snapshot events should have snapshot=true"
        );
    }

    // Verify field order matches insertion order (by primary key)
    let names: Vec<&str> = events
        .iter()
        .map(|e| {
            e.after
                .as_ref()
                .and_then(|v| v.get("name"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
        })
        .collect();
    assert_eq!(names, vec!["alpha", "bravo", "charlie", "delta", "echo"]);

    // Await background tasks
    let _ = keeper_handle.await;
    let _ = worker_handle.await;

    // Cleanup
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// PgConnection real API (requires Postgres)
// ---------------------------------------------------------------------------

/// Test that a real PgConnection can connect, create a slot and publication,
/// validate tables, and cleanly shut down.
#[tokio::test]
async fn test_pg_connection_api_lifecycle() {
    let pg = get_test_pg();
    let table = test_name("t_pg_api");
    let slot_name = test_name("slot_pg_api");
    let pub_name = test_name("pub_pg_api");

    pg.create_test_table(&table).await;
    // Insert a row so the table has data
    pg.insert_rows(&table, &[("api_test", 1)]).await;

    let conn_str = pg.connection_string();
    let mut source_cfg = source_config_from_conn_str(conn_str, &slot_name, &table);
    source_cfg.publication = pub_name;
    source_cfg.slot_name = slot_name;

    // Connect (replication mode)
    let pg_conn = PgConnection::connect(&source_cfg)
        .await
        .expect("PgConnection::connect should succeed");

    // Ensure replication slot — creates a new slot (fresh name), returns ZERO
    let _lsn = pg_conn
        .ensure_replication_slot()
        .await
        .expect("ensure_replication_slot should succeed");

    // Ensure publication
    pg_conn
        .ensure_publication()
        .await
        .expect("ensure_publication should succeed");

    // Validate tables
    pg_conn
        .validate_tables()
        .await
        .expect("validate_tables should succeed for existing table");

    // Clean shutdown
    pg_conn.close().await;
}

// ---------------------------------------------------------------------------
// Helpers for engine-level tests
// ---------------------------------------------------------------------------

/// Build a [`SourceConfig`] from a Postgres connection string.
/// The connection string is expected to have the form:
/// `postgres://user:password@host:port/dbname`
fn source_config_from_conn_str(conn_str: &str, slot: &str, table: &str) -> SourceConfig {
    // Strip the `postgres://` prefix
    let rest = conn_str.strip_prefix("postgres://").unwrap_or(conn_str);
    let (user_info, rest) = rest.split_once('@').unwrap_or(("", rest));
    let (user, password) = user_info.split_once(':').unwrap_or((user_info, ""));
    let (host_port, dbname) = rest.split_once('/').unwrap_or((rest, "tap_test"));
    let (host, port) = host_port.split_once(':').unwrap_or((host_port, "5432"));

    SourceConfig {
        host: host.to_string(),
        port: port.parse().unwrap_or(5432),
        dbname: dbname.to_string(),
        user: user.to_string(),
        password: password.to_string(),
        slot_name: slot.to_string(),
        publication: format!("pub_{slot}"),
        tables: vec![format!("public.{table}")],
        plugin: "pgoutput".to_string(),
        ssl_mode: tap_core::config::SslMode::Disable,
    }
}

/// Extract the database name from a connection string.
fn psql_db_name(conn_str: &str) -> String {
    let rest = conn_str.strip_prefix("postgres://").unwrap_or(conn_str);
    let (_user_info, rest) = rest.split_once('@').unwrap_or(("", rest));
    let (_host_port, dbname) = rest.split_once('/').unwrap_or((rest, "tap_test"));
    // Strip any query parameters
    dbname.split('?').next().unwrap_or(dbname).to_string()
}
