# Tap

**PostgreSQL Change Data Capture**

[![Crates.io](https://img.shields.io/crates/v/tap-core.svg)](https://crates.io/crates/tap-core)
[![npm](https://img.shields.io/npm/v/%40tap%2Fsdk)](https://www.npmjs.com/package/@tap/sdk)
[![CI](https://img.shields.io/github/actions/workflow/status/tap-dev/tap/ci.yml?branch=main)](https://github.com/tap-dev/tap/actions)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-blue)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

Tap tracks row-level changes in PostgreSQL via logical replication slots, serializes them into structured `ChangeEvent` envelopes (Debezium-compatible), and delivers them over HTTP/2 Server-Sent Events or in-process callbacks. State is persisted in a local SQLite store for checkpointed, idempotent delivery.

---

## Quickstart

Get change events from a Postgres database in under five minutes.

```bash
# Install the CLI
curl -fsSL https://tap.dev/install.sh | sh

# Or install the TypeScript SDK
npm install @tap/sdk

# Scaffold a project
tap init --db "postgres://user:pass@localhost:5432/mydb"

# Start capturing changes
tap capture

# In another terminal, consume the event stream
curl -N http://127.0.0.1:54321/events
```

---

## Example

```typescript
import { Tap } from "@tap/sdk";

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

The engine connects to Postgres via logical replication (`pgoutput` by default, `wal2json` experimental), decodes WAL records into structured events, and delivers them through two parallel paths: an in-process callback (TypeScript SDK) and an HTTP/2 SSE server. The SQLite state store persists LSN checkpoints, snapshot progress, and schema cache, enabling resume from the last confirmed position after a restart.

---

## Features

- **PostgreSQL logical replication** -- tracks inserts, updates, and deletes via built-in `pgoutput` protocol; `wal2json` support for managed Postgres environments
- **Structured ChangeEvent envelope** -- Debezium-compatible JSON format with `op`, `before`, `after`, `source`, `ts_ms`, and deterministic `id` fields
- **Dual delivery** -- events reach consumers through in-process TypeScript callbacks (napi-rs) and parallel HTTP/2 SSE stream, both fed from the same ordered source
- **Idempotent delivery** -- deterministic event IDs derived from Postgres LSN + transaction ID; consumers deduplicate by tracking processed IDs
- **Checkpointed state** -- SQLite state store with WAL mode, batch checkpointing, and integrity verification; resume from last confirmed LSN after restart
- **Sequential snapshotting** -- consistent point-in-time snapshot via `pg_export_snapshot()`, emitting `op: "r"` events with progress tracking and resume support
- **TypeScript SDK** -- native Node.js bindings via napi-rs; `Tap` class with `start`/`stop`/`pause`/`resume`/`status` and typed event callbacks
- **SSE with 8 event types** -- `change`, `heartbeat`, `snapshot_start`, `snapshot_progress`, `snapshot_complete`, `streaming_start`, `error`, `shutdown`; supports `Last-Event-ID` resume
- **CLI with 5 commands** -- `init`, `capture`, `inspect`, `dev`, `test` for project scaffolding, capture management, schema introspection, and local development
- **Graceful shutdown** -- SIGINT/SIGTERM flushes final LSN checkpoint, closes connections, and exits without data loss
- **Automatic reconnection** -- exponential backoff (1s to 60s, with jitter) on Postgres connection loss; up to 10 retries before exit

---

## Installation

### CLI Binary

```bash
curl -fsSL https://tap.dev/install.sh | sh
```

Prebuilt binaries are available for:

| Platform | Architecture |
|----------|-------------|
| macOS | ARM64 (Apple Silicon), x86_64 (Intel) |
| Linux | x86_64 (glibc), x86_64 (musl), ARM64 (glibc) |

### TypeScript SDK

```bash
npm install @tap/sdk
```

The SDK ships as a native Node.js addon compiled via `napi-rs`. The correct platform binary is selected automatically at install time via `@napi-rs/cli` artifact resolution.

**Prerequisites:**

- PostgreSQL 14+ (for `pgoutput` protocol; `wal2json` requires the `wal2json` extension)
- Node.js 22.5+ (for SDK usage)
- Rust 1.85+ (for building from source)

---

## CLI Reference

| Command | Description |
|---------|-------------|
| `tap init --db <CONNECTION>` | Scaffold a new Tap project: creates `.tap/config.toml`, validates Postgres credentials, creates replication slot and publication, generates TypeScript type definitions |
| `tap capture` | Start capturing CDC events: loads config, connects to Postgres, runs snapshot if needed, streams WAL changes, serves SSE endpoint |
| `tap inspect` | Introspect database schema and generate TypeScript type definitions |
| `tap dev` | Local development server with capture loop and status dashboard |
| `tap test` | Validate fixture files and replay events through the pipeline |

Use `tap <command> --help` for full option details.

---

## Configuration

Tap reads a TOML configuration file from `.tap/config.toml` by default. Key sections:

```toml
[source]
connection = "postgres://user:password@localhost:5432/mydb"
slot_name = "tap_slot"
publication = "tap_publication"
plugin = "pgoutput"

[state]
path = ".tap/state.db"

[sink]
host = "127.0.0.1"
port = 0

[capture]
max_batch_size = 1000
flush_interval_ms = 1000

[snapshot]
enabled = true

[reconnect]
max_retries = 10
initial_backoff_ms = 1000
```

---

## SDK API

```typescript
import { Tap, type TapConfig, type ChangeEvent } from "@tap/sdk";

interface TapConfig {
  connection: string;           // Postgres connection string
  slotName?: string;            // Replication slot name (default: "tap_slot")
  publication?: string;         // Publication name (default: "tap_publication")
  tables?: string[];            // Tables to capture (empty = all)
  plugin?: "pgoutput" | "wal2json";
  host?: string;                // SSE server host
  port?: number;                // SSE server port (0 = ephemeral)
  statePath?: string;           // SQLite state path
  maxBatchSize?: number;        // Events per batch before flush
  flushIntervalMs?: number;     // Max ms between checkpoints
  sslMode?: string;             // "disable" | "require" | "verify-ca" | "verify-full"
  sink?: SinkConfig;            // SSE server configuration
}
```

**Tap instance methods:**

| Method | Returns | Description |
|--------|---------|-------------|
| `start()` | `Promise<string>` | Connect to Postgres, ensure slot/publication, start WAL streaming, return SSE URL |
| `stop()` | `Promise<void>` | Flush checkpoint, close connections, stop SSE server |
| `pause()` | `Promise<void>` | Pause WAL reading (keep connections open) |
| `resume()` | `Promise<void>` | Resume WAL reading after pause |
| `status()` | `CaptureStatus` | Return current state, event count, LSN, and lag |
| `onChange(handler)` | `void` | Register callback for every row-level change event |
| `onError(handler)` | `void` | Register callback for capture errors |

---

## Project Status

**Version 0.1.0 "Foundation"** -- the core engine works. This release proves the capture pipeline from PostgreSQL to structured JSON events over SSE, with a TypeScript SDK and CLI for local development.

### What is included

- PostgreSQL CDC via logical replication (`pgoutput` first-class, `wal2json` experimental)
- SQLite state store with WAL mode, batch checkpointing, and integrity checks
- Sequential snapshotting with `pg_export_snapshot()` for consistent point-in-time views
- HTTP/2 SSE sink with 8 event types and `Last-Event-ID` resume
- CLI with 5 commands: `init`, `capture`, `inspect`, `dev`, `test`
- TypeScript SDK via napi-rs native bindings with in-process callbacks
- Deterministic event IDs for consumer-side deduplication
- Automatic reconnection with exponential backoff
- Graceful shutdown with checkpoint persistence
- Prebuilt binaries for macOS (ARM64, x86_64) and Linux (x86_64 glibc/musl, ARM64 glibc)

### What is deferred

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
# Install dependencies
moon setup

# Build all targets
moon run build

# Run tests
moon run test

# Run linting
moon run lint
```

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

---

## License

Apache 2.0. See [LICENSE](LICENSE) for details.
