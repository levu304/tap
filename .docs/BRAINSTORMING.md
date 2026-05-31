# Tap - Brainstorming Summary

> **Project:** Tap (formerly ChronoStream)  
> **Date:** 2026-05-31  
> **Status:** Architecture & Design Phase

---

## 1. Vision

Build a next-generation Change Data Capture (CDC) platform that inherits Debezium's architectural wisdom while eliminating its operational and ergonomic debt. Designed for modern developer workflows, polyglot teams, and AI-native operations.

### Core Principles
- **Embedded/Sidecar-first**: Runs as a 10MB binary next to your app, not a separate data platform
- **Polyglot-native**: First-class TypeScript, Python, Go, Rust, Elixir, Java, C# SDKs
- **AI-agent friendly**: MCP-native, self-describing APIs, natural-language pipeline generation
- **Transport-agnostic**: HTTP/2, WebSockets, gRPC, Redis, NATS, Kafka — not Kafka-locked
- **Fully open source**: Apache 2.0, zero open-core

---

## 2. Name

**Tap** — three letters, zero friction in the terminal. A common English word that perfectly describes the action (tapping into a database), joining the ranks of `git`, `curl`, and `zip`.

---

## 3. Requirements Analysis

### Inherit from Debezium
| Strength | How Tap Keeps It |
|----------|------------------|
| Log-based CDC | Core engine reads WAL/binlog/logical decoding directly |
| Standardized event envelope | Uniform `ChangeEvent` schema across all databases |
| Exactly-once guarantees | Idempotent consumers with deterministic event IDs |
| Snapshot + streaming | Concurrent, resumable snapshotting with seamless transition |
| Schema evolution tracking | DDL events captured as first-class change events |

### Resolve Debezium's Cons
| Debezium Pain Point | Tap Solution |
|---------------------|--------------|
| Kafka Connect lock-in | Transport-agnostic core; Kafka is one of many outputs |
| Java-only ecosystem | Rust core with native SDKs for TS, Python, Go, Elixir |
| Operational complexity | Sidecar/embedded-first; no ZooKeeper, no Kafka cluster required |
| YAML-heavy config | Code-first configuration with type-safe DSLs |
| Slow snapshots | Parallel, chunked snapshotting with configurable concurrency |
| No built-in transforms | Edge transformations in TS/Python/Wasm before leaving the node |
| Schema evolution is manual | Auto-adaptive schemas with optional migration hooks |
| High infra cost | Serverless-native: stateless capture agents + serverless state backend |

---

## 4. Architecture

### Three Deployment Modes

```
┌─────────────────────────────────────────────────────────────┐
│  EMBEDDED MODE   │  SIDECAR MODE    │  CLUSTER MODE       │
│  (Library)       │  (Container)     │  (Distributed)      │
├─────────────────────────────────────────────────────────────┤
│  import { tap }  │  docker run tap  │  tap cluster join   │
│  from '@tap/sdk' │    --sidecar     │    --discovery=...  │
│                  │                  │                     │
│  const t = new   │  Localhost:9911  │  HA coordinator     │
│    Tap({...})    │  HTTP API +      │  Shard per DB       │
│                  │  Unix socket     │  Auto-failover      │
│  t.on('change',  │                  │                     │
│    handler       │  Your app talks  │  Global metrics     │
│  )               │  to localhost    │  & alerting         │
└─────────────────────────────────────────────────────────────┘
```

### Core Components

| Component | Technology | Responsibility |
|-----------|------------|----------------|
| **Capture Engine** | Rust | Zero-copy log parsing for Postgres, MySQL, SQL Server, MongoDB, SQLite |
| **Stream Processor** | WebAssembly + QuickJS | User-defined transforms in TS/Python/Wasm sandbox |
| **Transport Layer** | Rust (async) | HTTP/2 SSE, gRPC, WebSocket, Redis, NATS, Kinesis, Pub/Sub, Kafka |
| **Schema Registry** | SQLite/Postgres (embedded) | Schema versions, auto-migration SQL generation, semantic diffing |
| **State Store** | Tiered (see §7) | LSN checkpoints, snapshot progress, DLQ metadata |

---

## 5. Monorepo & Tech Stack

### Tool: moon (moonrepo.dev)

Chosen for first-class polyglot support, deterministic task graphs, and remote caching across Rust, Node, Python, Go, Bun, and Elixir.

### Repository Layout

```
tap/
├── .moon/
│   ├── toolchain.yml          # Global tool versions
│   └── tasks.yml              # Shared task templates
├── .prototools                # proto version manager
├── crates/                    # Rust workspace (Cargo)
│   ├── tap-core/              # Core capture engine + state + WASM runtime
│   ├── tap-ffi/               # C FFI layer (libtap_core.so / .dll / .dylib)
│   ├── tap-cli/               # CLI binary (`tap` command)
│   ├── tap-sidecar/           # Sidecar server (gRPC + HTTP)
│   ├── tap-wasm/              # WASM module for edge transforms
│   └── tap-mcp/               # MCP server for AI agents
├── packages/                  # SDKs and non-Rust packages
│   ├── sdk-ts/                # Node/TypeScript SDK (napi-rs)
│   ├── sdk-py/                # Python SDK (pyo3 + maturin)
│   ├── sdk-go/                # Go SDK (CGO to C FFI)
│   ├── sdk-elixir/            # Elixir SDK (Rustler NIF)
│   ├── sdk-java/              # Java/Kotlin SDK (gRPC client)
│   ├── sdk-dotnet/            # C# SDK (gRPC client)
│   ├── sdk-ruby/              # Ruby SDK (gRPC client)
│   ├── sdk-php/               # PHP SDK (gRPC client)
│   └── proto/                 # Shared protobuf schemas
├── scripts/                   # CI/CD, release automation
└── docs/                      # Documentation site
```

### Cross-Language Task Graph (moon)

```yaml
# Example: sdk-ts depends on tap-ffi build
sdk-ts:
  tasks:
    build:
      command: 'pnpm exec napi build --platform --release'
      deps:
        - 'tap-ffi:build'
        - 'proto:generate'
```

### Toolchain Versions
- **Rust**: 1.85.0 (with `wasm32-unknown-unknown` target)
- **Node**: 22.5.0 + pnpm 9.15.0
- **Python**: 3.12.0
- **Go**: 1.24.0
- **Bun**: 1.2.0

---

## 6. SDK Strategy

### Tier 1: Native Embedded (Zero Sidecar)
| Language | Binding | Why |
|----------|---------|-----|
| **TypeScript** | napi-rs | Async-first, auto-generated TS defs, cross-platform prebuilds |
| **Python** | pyo3 + maturin | Native asyncio, GIL-safe, pip-installable wheels |
| **Rust** | Native crate | Zero-cost transforms, `tokio` async streams |
| **Go** | CGO to C FFI | Channels for streaming, static binary compilation |
| **Elixir** | Rustler NIF | BEAM's GenStage/Flow is architecturally perfect for CDC |

### Tier 2: Sidecar-First, Native Optional
| Language | Binding | Integration |
|----------|---------|-------------|
| **Java/Kotlin/Scala** | gRPC client | Spring Boot Starter, Micronaut, Reactor/RxJava |
| **C#/.NET** | gRPC client | `IAsyncEnumerable`, `IHostedService`, `IObservable` |

### Tier 3: Sidecar-Only
| Language | Binding |
|----------|---------|
| **Ruby** | gRPC client (block-based callbacks) |
| **PHP** | gRPC client |
| **Others** | OpenAPI spec + gRPC reflection for community generation |

### Universal Protocol
All SDKs expose the same 5 operations:
1. `capture(config)` → Stream
2. `snapshot(config)` → Resumable Task
3. `getSchema(table)` → JSON Schema
4. `getMetrics()` → OpenTelemetry metrics
5. `control(command)` → Pause / Resume / Rewind

---

## 7. State Storage Recommendation

**Tiered "SQLite-by-Default" Architecture**

| Tier | Mode | Backend | What It Stores |
|------|------|---------|----------------|
| **Hot Local** | Embedded / Sidecar | **SQLite** (`.tap/state.db`) | LSN checkpoints, snapshot chunk progress, schema versions, DLQ metadata |
| **Shared** | Cluster | **Postgres** (existing) | Cross-node offsets, global schema registry, leader elections, heartbeats |
| **Cold / Serverless** | Serverless | **S3/R2** + lightweight locking | Immutable checkpoint archives, long-term schema history |
| **Coordination** | Cluster | **Built-in gossip or Raft** | Cluster membership, shard allocation, fail-over |

**Recommendation:** SQLite default for 90% of users. A local file is sufficient because CDC state is tiny (just offsets and schema pointers). Only escalate to Postgres when running clustered. Use a unified `StateBackend` trait in Rust so the core engine is identical regardless of backend.

---

## 8. Global Ordering

**Source-Native Global Order (Free)**
- Use the source database's native LSN/commit timestamp as the canonical `sequence` field
- Postgres WAL, MySQL binlog position, SQL Server LSN already provide a total order

**Cross-Source / Cross-Shard Global Order (Opt-in)**
- Assign every event a **Hybrid Logical Clock (HLC)** — `(wall_time, logical_counter)`
- AI agents merge streams by HLC; same HLC = concurrent
- Optional lightweight **Sequencer Node** (single-threaded allocator, Raft if HA needed)

Event envelope:
```json
{
  "source_sequence": "0/16B37428",
  "hlc": "2026-05-31T13:54:00.123Z:00042",
  "vector_clock": {"db1": 45, "db2": 12}
}
```

---

## 9. AI-Agent Friendly Design

### MCP (Model Context Protocol) Native
Tap exposes an MCP server so AI agents can:
- **Discover** database schemas and relationships automatically
- **Generate** pipeline code from natural language
- **Debug** by querying live metrics and LSN positions
- **Heal** by suggesting config changes and proposing fixes

### Self-Describing APIs
Every component exposes JSON Schema + OpenAPI. Agents can introspect and manipulate pipelines without human-written documentation.

### Natural Language to Pipeline
```typescript
const pipeline = await tap.ai.generate({
  prompt: `
    Capture changes from the inventory database.
    When stock drops below 10, emit a low_stock alert.
    Send everything to Redis stream 'inventory.updates'.
    Mask supplier prices from alerts.
  `,
  review: true, // Human approval gate
});
```

---

## 10. License & Business Model

- **License:** Apache 2.0 (single license, no dual licensing, no open-core)
- **Open governance:** BDFL model initially, path to neutral foundation by v2.0
- **Business model:** Managed Cloud (pay-per-event hosting) + Enterprise Support + Certification program
- **No proprietary "enterprise edition"** — all features in the repo

---

## 11. Implementation Roadmap

| Phase | Timeline | Deliverables |
|-------|----------|--------------|
| **1** | Months 1-3 | Rust capture engine (Postgres + MySQL), Embedded TS/Python SDKs, HTTP/2 SSE + WebSocket sinks, Local dev CLI with hot reload |
| **2** | Months 4-6 | Sidecar mode, Cluster mode with discovery, Parallel snapshotting, Schema registry + auto-migrations, OpenTelemetry observability |
| **3** | Months 7-9 | Go + Elixir native SDKs, WASM transform marketplace, MCP server, Natural language pipeline generation, Managed cloud offering |
| **4** | Months 10-12 | SQL Server, Oracle, MongoDB connectors, Java/C# native bindings (Project Panama / P/Invoke), RBAC + audit logging, SOC 2 |

---

## 12. Open Questions to Resolve

1. **Transform language runtime:** QuickJS for TS transforms, but Python transforms need Pyodide (WASM) or a separate Python micro-VM. Performance vs. fidelity tradeoff.
2. **WASM edge deployment:** Compile the entire capture engine to WASM for Cloudflare Workers / Deno Deploy. Requires WebSocket proxy for Postgres logical decoding.
3. **Java native binding path:** Project Panama (Java 22+) vs. JNA for older LTS. Decision deferred to Phase 4.
4. **Sequencer Node consensus:** Built-in gossip sufficient for most cases, or always use Raft for the Sequencer Node?

---

*Generated during brainstorming session on 2026-05-31.*
