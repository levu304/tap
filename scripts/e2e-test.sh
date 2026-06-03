#!/usr/bin/env bash
# ----------------------------------------------------------------------------
# E2E test for Tap — PostgreSQL Change Data Capture engine
#
# Prerequisites:
#   - psql (Postgres client)
#   - cargo (Rust toolchain)
#   - A running Postgres instance with logical replication configured
#     (wal_level=logical, max_replication_slots=10, max_wal_senders=10)
#
# Environment:
#   TAP_TEST_DB  Postgres connection string (default: postgres://postgres:tap_test@localhost:5432/tap_test)
#
# Usage:
#   ./scripts/e2e-test.sh
#
# Exit code 0 on success, non-zero on failure.
# ----------------------------------------------------------------------------
set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
TEST_DB="${TAP_TEST_DB:-postgres://postgres:tap_test@localhost:5432/tap_test}"
TEST_TABLE="e2e_test_events"
TEMP_CONFIG=$(mktemp /tmp/tap_e2e_config.XXXXXX.toml)
TEMP_OUTPUT=$(mktemp /tmp/tap_e2e_output.XXXXXX)
CAPTURE_TIMEOUT=10
CAPTURE_PID=""

cleanup() {
    echo "=== Cleaning up ==="
    # Kill capture process if still running
    if [[ -n "$CAPTURE_PID" ]] && kill -0 "$CAPTURE_PID" 2>/dev/null; then
        kill "$CAPTURE_PID" 2>/dev/null || true
        wait "$CAPTURE_PID" 2>/dev/null || true
    fi
    # Remove temp files
    rm -f "$TEMP_CONFIG" "$TEMP_OUTPUT"
    echo "=== Cleanup done ==="
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Prerequisites check
# ---------------------------------------------------------------------------
echo "=== Checking prerequisites ==="

if ! command -v psql &>/dev/null; then
    echo "ERROR: psql not found. Install PostgreSQL client tools."
    exit 1
fi

if ! command -v cargo &>/dev/null; then
    echo "ERROR: cargo not found. Install the Rust toolchain."
    exit 1
fi

# Check Postgres connectivity
# Redact password in log output
echo "Testing Postgres connection to: $(echo "$TEST_DB" | sed 's/:[^:@]*@/:****@/')"
if ! psql "$TEST_DB" -c "SELECT 1" &>/dev/null; then
    echo "ERROR: Cannot connect to Postgres at $TEST_DB"
    echo "       Ensure Postgres is running and the connection string is correct."
    exit 1
fi

# Check wal_level is logical
WAL_LEVEL=$(psql "$TEST_DB" -Atc "SHOW wal_level" 2>/dev/null || echo "")
if [[ "$WAL_LEVEL" != "logical" ]]; then
    echo "ERROR: wal_level must be 'logical', got '$WAL_LEVEL'"
    echo "       Set wal_level=logical in postgresql.conf and restart."
    exit 1
fi

echo "Prerequisites OK"

# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------
echo "=== Setting up test database ==="

# Drop and recreate the test database
# Extract database name from connection string
DB_NAME=$(psql "$TEST_DB" -Atc "SELECT current_database()")
echo "Working with database: $DB_NAME"

# Drop and recreate test table
psql "$TEST_DB" <<SQL
DROP TABLE IF EXISTS $TEST_TABLE;
CREATE TABLE $TEST_TABLE (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    value BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
SQL

# Insert some test data
psql "$TEST_DB" <<SQL
INSERT INTO $TEST_TABLE (name, value) VALUES
    ('alpha', 100),
    ('beta', 200),
    ('gamma', 300);
SQL

echo "Inserted 3 test rows into $TEST_TABLE"

# ---------------------------------------------------------------------------
# Build the project
# ---------------------------------------------------------------------------
echo "=== Building Tap ==="
cargo build --workspace 2>&1 | tail -5
echo "Build complete"

# ---------------------------------------------------------------------------
# Create Tap config
# ---------------------------------------------------------------------------
echo "=== Creating Tap config ==="
cat >"$TEMP_CONFIG" <<TOML
[source]
host = "localhost"
port = 5432
dbname = "${DB_NAME}"
user = "postgres"
password = "tap_test"
slot_name = "tap_e2e_slot"
publication = "tap_e2e_pub"
tables = ["public.${TEST_TABLE}"]
plugin = "pgoutput"
ssl_mode = "disable"

[sink]
host = "127.0.0.1"
port = 0
max_buffer_size = 10000
heartbeat_interval_ms = 30000

[capture]
max_batch_size = 100
flush_interval_ms = 1000
snapshot = true

[snapshot]
batch_size = 1000

[state]
path = "/tmp/tap_e2e_state.db"
max_backup_size_kb = 1024

[logging]
format = "json"
level = "info"
TOML

echo "Config written to $TEMP_CONFIG"

# ---------------------------------------------------------------------------
# Run Tap capture
# ---------------------------------------------------------------------------
echo "=== Starting Tap capture ==="

# Run capture in background, capture output
cargo run -- capture --config "$TEMP_CONFIG" >"$TEMP_OUTPUT" 2>&1 &
CAPTURE_PID=$!

echo "Capture PID: $CAPTURE_PID"

# Wait for capture to produce output (events)
sleep "$CAPTURE_TIMEOUT"

# Check if capture is still running
if kill -0 "$CAPTURE_PID" 2>/dev/null; then
    echo "Capture is running, stopping..."
    kill "$CAPTURE_PID" 2>/dev/null || true
    wait "$CAPTURE_PID" 2>/dev/null || true
else
    echo "Capture has exited."
fi

# ---------------------------------------------------------------------------
# Verify results
# ---------------------------------------------------------------------------
echo "=== Verifying results ==="

CAPTURE_OUTPUT=$(cat "$TEMP_OUTPUT")
echo "$CAPTURE_OUTPUT"

# Check for expected output patterns
if echo "$CAPTURE_OUTPUT" | grep -qi "error\|panic\|failed"; then
    echo "FAILURE: Capture output contains errors"
    exit 1
fi

if echo "$CAPTURE_OUTPUT" | grep -qi "snapshot\|ChangeEvent\|event"; then
    echo "SUCCESS: Events were captured during the test run"
else
    echo "FAILURE: No snapshot/event messages found in output"
    echo "         At least one CDC event is expected (3 rows were pre-inserted"
    echo "         and snapshot mode is enabled)."
    exit 1
fi
