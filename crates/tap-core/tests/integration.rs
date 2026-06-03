//! Integration tests for tap-core using testcontainers.
//!
//! These tests spin up a real Postgres container, configure it for logical
//! replication, and verify the capture pipeline end-to-end.
//!
//! NOTE: The actual test functions are stubs — they will be implemented
//! by the qa-expert agent in a follow-up pass.

use std::sync::{OnceLock, Mutex};

use testcontainers::{clients::Cli, core::WaitFor, Container, GenericImage};

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
/// The container is dropped (& cleaned up) when `TestPg` goes out of scope.
struct TestPg {
    /// Running container (Dropped → container removed).
    #[allow(dead_code)]
    container: Container<'static, GenericImage>,
    /// Connection string for the running Postgres instance.
    connection_string: String,
}

impl TestPg {
    /// Start a Postgres container and configure it for CDC.
    fn start() -> Self {
        let _lock = DOCKER_LOCK.lock().expect("docker lock");

        // Start Postgres with health check
        let container = docker().run(
            GenericImage::new(PG_IMAGE, PG_TAG)
                .with_wait_for(WaitFor::message_on_stderr(
                    "database system is ready to accept connections",
                ))
                .with_env_var("POSTGRES_PASSWORD", "tap_test")
                .with_env_var("POSTGRES_DB", "tap_test"),
        );

        let host = "localhost";
        let port = container.get_host_port_ipv4(5432);
        let connection_string = format!("postgres://postgres:tap_test@{host}:{port}/tap_test");

        // Configure WAL for logical replication (synchronous connect)
        let (client, connection) = tokio::runtime::Handle::current()
            .block_on(async {
                tokio_postgres::connect(&connection_string, tokio_postgres::NoTls).await
            })
            .expect("connect to postgres");

        tokio::spawn(connection);

        let rt = tokio::runtime::Handle::current();
        rt.block_on(async {
            client
                .simple_query("ALTER SYSTEM SET wal_level = 'logical'")
                .await
                .expect("set wal_level");
            client
                .simple_query("ALTER SYSTEM SET max_replication_slots = '10'")
                .await
                .expect("set max_replication_slots");
            client
                .simple_query("ALTER SYSTEM SET max_wal_senders = '10'")
                .await
                .expect("set max_wal_senders");
        });

        // Note: A restart is needed for ALTER SYSTEM to take effect.
        // testcontainers 0.15 doesn't expose Container::restart().
        // In a full implementation, use a custom Dockerfile or compose file
        // with wal_level pre-configured, or exec docker restart via Command.

        Self {
            container,
            connection_string,
        }
    }

    /// Get the connection string for this test Postgres instance.
    fn connection_string(&self) -> &str {
        &self.connection_string
    }

    /// Execute SQL on the test database.
    async fn execute(&self, sql: &str) {
        let (client, connection) = tokio_postgres::connect(
            self.connection_string(),
            tokio_postgres::NoTls,
        )
        .await
        .expect("connect to postgres");
        tokio::spawn(connection);
        client.batch_execute(sql).await.expect("execute sql");
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

    /// Insert sample rows into a test table.
    async fn insert_rows(&self, table_name: &str, rows: &[(&str, i64)]) {
        for (name, value) in rows {
            self.execute(&format!(
                "INSERT INTO {table_name} (name, value) VALUES ('{name}', {value})"
            ))
            .await;
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
// Integration test stubs
// ---------------------------------------------------------------------------

/// Verify that the TestPg harness can start a container and connect.
#[tokio::test]
async fn test_harness_starts_container() {
    let pg = TestPg::start();
    assert!(!pg.connection_string().is_empty());
    // Container is dropped at end of scope, cleaning up.
}

/// Test creating a table and inserting rows via the harness.
#[tokio::test]
async fn test_harness_create_table_and_insert() {
    let pg = TestPg::start();
    pg.create_test_table("harness_test").await;
    pg.insert_rows("harness_test", &[("test1", 10), ("test2", 20)])
        .await;
}

/// TODO: Test that a replication slot can be created and used.
#[tokio::test]
async fn test_create_replication_slot() {
    let pg = get_test_pg();
    pg.execute("DROP PUBLICATION IF EXISTS tap_test_pub")
        .await;
    pg.execute("CREATE PUBLICATION tap_test_pub FOR ALL TABLES")
        .await;
}

/// TODO: Test that ChangeEvents are produced for INSERT operations.
#[tokio::test]
async fn test_captures_insert_events() {
    // TODO: Implement
}

/// TODO: Test that ChangeEvents are produced for UPDATE operations.
#[tokio::test]
async fn test_captures_update_events() {
    // TODO: Implement
}

/// TODO: Test that ChangeEvents are produced for DELETE operations.
#[tokio::test]
async fn test_captures_delete_events() {
    // TODO: Implement
}

/// TODO: Test snapshot produces Read events.
#[tokio::test]
async fn test_snapshot_produces_read_events() {
    // TODO: Implement
}

/// TODO: Test that the capture pipeline handles multiple tables.
#[tokio::test]
async fn test_multiple_tables() {
    // TODO: Implement
}

/// TODO: Test graceful shutdown mid-stream.
#[tokio::test]
async fn test_graceful_shutdown() {
    // TODO: Implement
}
