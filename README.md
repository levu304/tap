# Tap

**PostgreSQL Change Data Capture**

[![Crates.io](https://img.shields.io/crates/v/tap-core.svg)](https://crates.io/crates/tap-core)
[![npm](https://img.shields.io/npm/v/tap-cdc)](https://www.npmjs.com/package/tap-cdc)
[![CI](https://img.shields.io/github/actions/workflow/status/levu304/tap/ci.yml?branch=main)](https://github.com/levu304/tap/actions)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-blue)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

Tap tracks row-level changes in PostgreSQL via logical replication slots, serializes them into structured `ChangeEvent` envelopes (Debezium-compatible), and delivers them over HTTP/1.1 Server-Sent Events or in-process callbacks. State is persisted in a local SQLite store for checkpointed, idempotent delivery.

---

## Quickstart

Get change events from a Postgres database in under five minutes.

```bash
# Option A: Install the CLI binary
curl -fsSL https://levu304.github.io/tap/install.sh | sh

# Scaffold a project
tap init --db mydb --user postgres --password secret

# Start capturing changes
tap capture

# In another terminal, consume the event stream
curl -N http://127.0.0.1:8080/events
```

```typescript
// Option B: Install the TypeScript SDK (library — no CLI)
npm install tap-cdc

import { Tap } from "tap-cdc";

const tap = new Tap({
  connection: "postgresql://user:pass@localhost/mydb",
  tables: ["public.users", "public.orders"],
});

tap.onChange((event) => {
  console.log(`[${event.op}] ${event.source.table}`, event.after);
});

await tap.start();
```

---

## Example

```typescript
import { Tap } from "tap-cdc";

const tap = new Tap({
  connection: "postgresql://user:pass@localhost/mydb",
  tables: ["public.users", "public.orders"],
});

tap.onChange((event) => {
  console.log(`[${event.op}] ${event.source.table}`, event.after);
});

tap.onError((err) => {
  console.error("Capture error:", err);
});

const sseUrl = await tap.start();
console.log(`SSE endpoint: ${sseUrl}`);
```

Each `ChangeEvent` carries the operation type (`c` create, `u` update, `d` delete, `r` snapshot read), the row state before and after the change, source metadata (database, schema, table, LSN, transaction ID), and a deterministic event ID for consumer-side deduplication.

---

## Architecture

```
Postgres WAL  -->  WalDecoder  -->  ChangeEvent  -->  SSE Server  -->  HTTP clients
                                        |
                                    Callbacks
                                  (JavaScript
                                   via napi-rs)
                                        |
                                   SQLite State
                                   Store (LSN
                                   checkpoints)
```

The engine connects to Postgres via logical replication (`pgoutput` by default, `wal2json` experimental), decodes WAL records into structured events, and delivers them through two parallel paths: an in-process callback (TypeScript SDK) and an HTTP/1.1 SSE server. The SQLite state store persists LSN checkpoints, snapshot progress, and schema cache, enabling resume from the last confirmed position after a restart.

---

## Features

- **PostgreSQL logical replication** -- tracks inserts, updates, and deletes via built-in `pgoutput` protocol; `wal2json` support for managed Postgres environments
- **Structured ChangeEvent envelope** -- Debezium-compatible JSON format with `op`, `before`, `after`, `source`, `ts_ms`, and deterministic `id` fields
- **Dual delivery** -- events reach consumers through in-process TypeScript callbacks (napi-rs) and parallel HTTP/1.1 SSE stream, both fed from the same ordered source
- **Idempotent delivery** -- deterministic event IDs derived from Postgres LSN + transaction ID; consumers deduplicate by tracking processed IDs
- **Checkpointed state** -- SQLite state store with WAL mode, batch checkpointing, and integrity verification; resume from last confirmed LSN after restart
- **Sequential snapshotting** -- consistent point-in-time snapshot via `pg_export_snapshot()`, emitting `op: "r"` events with progress tracking and resume support
- **TypeScript SDK** -- native Node.js bindings via napi-rs; `Tap` class with `start`/`stop`/`pause`/`resume`/`status` and typed event callbacks
- **SSE with 8 event types** -- `change`, `heartbeat`, `snapshot_start`, `snapshot_progress`, `snapshot_complete`, `streaming_start`, `error`, `shutdown`
- **HTTP endpoints** -- `/events` (SSE stream), `/health` (liveness check), `/status` (capture state detail); optional API key auth via `Authorization: Bearer` or `X-Api-Key`
- **CLI with 5 commands** -- `init`, `capture`, `inspect`, `dev`, `test` for project scaffolding, capture management, and schema introspection
- **Graceful shutdown** -- SIGINT/SIGTERM flushes final LSN checkpoint, closes connections, and exits without data loss
- **Automatic reconnection** -- exponential backoff (500ms initial, 30s max, ±20% jitter) on Postgres connection loss; up to 10 retries before exit

---

## Installation

### CLI Binary

```bash
curl -fsSL https://levu304.github.io/tap/install.sh | sh
```

Prebuilt binaries are available for:

| Platform | Architecture |
|----------|-------------|
| macOS | ARM64 (Apple Silicon), x86_64 (Intel) |
| Linux | x86_64 (glibc), x86_64 (musl), ARM64 (glibc) |

### TypeScript SDK

```bash
npm install tap-cdc
```

The SDK ships as a native Node.js addon compiled via `napi-rs`. The correct platform binary is selected automatically at install time via optional dependency resolution.

**Prerequisites:**

- PostgreSQL 14+ (for `pgoutput` protocol; `wal2json` requires the `wal2json` extension)
- Node.js 22+ (for SDK usage)
- Rust 1.85+ (for building from source)

---

## CLI Reference

| Command | Description |
|---------|-------------|
| `tap init --db <NAME>` | Scaffold a new Tap project: creates `.tap/config.toml`, validates Postgres credentials, creates replication slot and publication; generates TypeScript type definitions if `--table` is specified |
| `tap capture` | Start capturing CDC events: loads config, connects to Postgres, runs snapshot if needed, streams WAL changes, serves SSE endpoint |
| `tap inspect` | Introspect database schema and generate TypeScript type definitions or JSON schema |
| `tap dev` | Alias for `tap capture` with enhanced terminal output; HTML status page is planned for a future release |
| `tap test` | List fixture files from `.tap/fixtures/` or validate that a single `.json` file is a valid `ChangeEvent` |

**Global flags** (available on all commands):

| Flag | Default | Description |
|------|---------|-------------|
| `-c, --config <PATH>` | `.tap/config.toml` | Path to TOML configuration file |
| `--log-level <LEVEL>` | `info` | Log level filter (`trace`, `debug`, `info`, `warn`, `error`); also reads `TAP_LOG` env var |
| `--log-format <FORMAT>` | `text` | Log format (`text` or `json`) |

Use `tap <command> --help` for full option details (CLI binary only).

---

## Configuration

Tap reads a TOML configuration file from `.tap/config.toml` by default. Key sections and their defaults:

```toml
[source]
host = "localhost"
port = 5432
dbname = "mydb"
user = "replicator"
password = "secret"
slotName = "tap_slot"
publication = "tap_publication"
tables = ["public.users", "public.orders"]  # empty = all tables in publication
plugin = "pgoutput"
ssl_mode = "disable"  # disable | require | verify-ca | verify-full

[sink]
host = "0.0.0.0"
port = 8080
maxBufferSize = 1000
heartbeatIntervalMs = 30000
# apiKey = "secret"  # optional; enables Bearer / X-Api-Key auth

[capture]
snapshot = true
maxBatchSize = 100
flushIntervalMs = 1000

[snapshot]
batchSize = 1000
numWorkers = 4

[state]
path = ".tap/state.db"
maxBackupSizeKb = 10240

[logging]
format = "json"
level = "info"
# file = "/var/log/tap.log"  # optional; writes to stderr when absent
```

---

## SDK API

```typescript
import { Tap, type TapConfig, type ChangeEvent } from "tap-cdc";

interface TapConfig {
  connection: string;           // Postgres connection string (overrides individual fields)
  slotName?: string;            // Replication slot name (default: "tap_slot")
  publication?: string;         // Publication name (default: "tap_publication")
  tables?: string[];            // Tables to capture (empty = all)
  plugin?: "pgoutput" | "wal2json";
  host?: string;                // Postgres hostname (default: "localhost")
  port?: number;                // Postgres port (default: 5432)
  database?: string;            // Database name
  user?: string;                // Replication user name
  password?: string;            // Replication user password
  statePath?: string;           // SQLite state path (default: ".tap/state.db")
  maxBatchSize?: number;        // Events per batch before flush (default: 100)
  flushIntervalMs?: number;     // Max ms between checkpoints (default: 1000)
  sslMode?: string;             // "disable" | "require" | "verify-ca" | "verify-full"
  sink?: SinkConfig;            // SSE server configuration
}
```

**Tap instance methods:**

| Method | Returns | Description |
|--------|---------|-------------|
| `start()` | `Promise<string>` | Connect to Postgres, ensure slot/publication, start WAL streaming, return SSE URL |
| `stop()` | `Promise<void>` | Flush checkpoint, close connections, stop SSE server |
| `pause()` | `Promise<void>` | Pause WAL reading (keep connections open); throws if not in `"streaming"` state |
| `resume()` | `Promise<void>` | Resume WAL reading after pause; throws if not in `"paused"` state |
| `status()` | `Promise<CaptureStatus>` | Return current state, event count, LSN, and lag |
| `onChange(handler)` | `void` | Register callback for every row-level change event (replaces any previous handler) |
| `onError(handler)` | `void` | Register callback for capture errors (replaces any previous handler) |

**Helper functions:**

```typescript
import { changeEventToJson } from "tap-cdc";

// Serialize a ChangeEvent to a formatted JSON string
const json = changeEventToJson(event);
```

---

## Project Status

**Version 0.1.0 "Foundation"** -- the core engine works. This release proves the capture pipeline from PostgreSQL to structured JSON events over SSE, with a TypeScript SDK and CLI for local development.

### What is included

- PostgreSQL CDC via logical replication (`pgoutput` first-class, `wal2json` experimental)
- SQLite state store with WAL mode, batch checkpointing, and integrity checks
- Sequential snapshotting with `pg_export_snapshot()` for consistent point-in-time views
- HTTP/1.1 SSE sink with 8 event types, `/health` and `/status` endpoints, and optional API key auth
- CLI with 5 commands: `init`, `capture`, `inspect`, `dev`, `test`
- TypeScript SDK via napi-rs native bindings with in-process callbacks
- Deterministic event IDs for consumer-side deduplication
- Automatic reconnection with exponential backoff (500ms–30s, ±20% jitter, up to 10 retries)
- Graceful shutdown with checkpoint persistence
- Prebuilt binaries for macOS (ARM64, x86_64) and Linux (x86_64 glibc/musl, ARM64 glibc)

### What is deferred

- `Last-Event-ID` resume for reconnecting SSE clients (v0.2.0)
- Sidecar deployment mode (v0.2.0)
- Python, Go, and other SDKs (v0.2.0+)
- Parallel snapshotting (v0.2.0)
- Transforms and WASM runtime (v0.2.0)
- Kafka, Redis, NATS, and gRPC transports (v0.2.0+)
- Schema registry and DDL capture (v0.3.0)
- Cluster mode (v0.3.0)
- TLS and authentication (v0.6.0)
- OpenTelemetry and dashboard UI (v0.3.0)

---

## Documentation

- [Implementation Plan](.docs/plans/v0.1.0-implementation-plan.md) -- detailed breakdown of all phases
- [Technical Specification](.docs/specs/v0.1.0-tech-spec.md) -- canonical design document
- [Roadmap](.docs/ROADMAP.md) -- version-by-version feature plan
- [TypeScript SDK Reference](packages/sdk-ts/src/index.ts) -- full API with JSDoc annotations

---

## Contributing

This project uses a [moon](https://moonrepo.dev) monorepo with Rust and JavaScript toolchains.

```bash
# Build all targets
moon run build

# Run tests
moon run test

# Run linting
moon run lint

# Or use cargo and pnpm directly
cargo build --workspace
cargo test --workspace
cargo clippy --workspace -- -D warnings
cargo fmt --all

cd packages/sdk-ts
pnpm install
pnpm build
pnpm exec vitest run
```

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

---

## License

Apache 2.0. See [LICENSE](LICENSE) for details.
