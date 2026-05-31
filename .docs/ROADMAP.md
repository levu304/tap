# Tap Roadmap

> **Project:** Tap  
> **Start Version:** 0.1.0  
> **Current Phase:** Foundation  
> **License:** Apache 2.0

---

## Versioning Philosophy

- **0.x.y**: Rapid iteration, APIs may change, production use at own risk
- **1.0.0**: Stable core API, backward compatibility guarantees, production-ready
- **Post-1.0**: Feature expansion, enterprise connectors, managed cloud

Each minor version (0.1.0 → 0.2.0) represents a **milestone** with a clear theme. Patch versions (0.1.1) are bug fixes and performance improvements.

---

## 0.1.0 — "Foundation" (Month 1-2)

**Theme:** Prove the core engine works. One database, one language, one transport.

### Deliverables

#### Core Engine (`tap-core`)
- [ ] Postgres logical decoding adapter (pgoutput / wal2json)
- [ ] WAL position tracking with SQLite state store
- [ ] Basic `ChangeEvent` envelope structure
- [ ] Exactly-once semantics via deterministic event IDs
- [ ] Sequential snapshotting (single-threaded, full table)
- [ ] Graceful shutdown with checkpoint persistence

#### CLI (`tap-cli`)
- [ ] `tap init` — scaffold a new Tap project
- [ ] `tap capture` — start capturing from Postgres
- [ ] `tap inspect` — inspect database schema and generate types
- [ ] `tap dev` — local dev server with hot reload
- [ ] `tap test` — test transforms with captured fixtures

#### TypeScript SDK (`sdk-ts`)
- [ ] Embedded mode via napi-rs
- [ ] Basic `Tap` class with `on('change')` callback API
- [ ] Auto-generated TypeScript types from live Postgres schema
- [ ] HTTP/2 SSE sink output
- [ ] Prebuilt binaries for macOS (ARM64/x86) and Linux (x86)

#### Infrastructure
- [ ] moon monorepo setup with Rust + Node toolchains
- [ ] CI pipeline (GitHub Actions) with `moon ci`
- [ ] Basic test suite: unit tests for core, integration tests for TS SDK
- [ ] README + quickstart guide

### Non-Goals (Explicitly Out of Scope)
- No sidecar mode yet
- No Python SDK yet
- No parallel snapshotting
- No transforms/WASM
- No Kafka/Redis/NATS — only HTTP/2 SSE
- No schema registry
- No cluster mode

### Success Criteria
- A developer can run `tap init && tap capture` against a local Postgres and receive change events over SSE within 5 minutes.
- Capture latency < 100ms for single-row inserts.
- No data loss on graceful shutdown.

---

## 0.2.0 — "Polyglot" (Month 2-3)

**Theme:** Expand to Python. Add sidecar mode. Introduce transforms.

### Deliverables

#### Core Engine
- [ ] MySQL binlog adapter (ROW-based replication)
- [ ] Parallel snapshotting for large tables (chunked by primary key range)
- [ ] Transform engine: QuickJS WASM runtime for TypeScript transforms
- [ ] Basic transform primitives: filter, map, mask (field-level redaction)

#### Python SDK (`sdk-py`)
- [ ] Embedded mode via pyo3 + maturin
- [ ] Asyncio-native `Tap` class
- [ ] Pydantic model generation from live schema
- [ ] Same feature parity as TS SDK (capture, inspect, dev server)

#### Sidecar Mode (`tap-sidecar`)
- [ ] HTTP API (REST + SSE) on localhost:9911
- [ ] Unix socket support for same-machine communication
- [ ] Health check endpoint (`GET /health`)
- [ ] Metrics endpoint (`GET /metrics` — Prometheus format)
- [ ] TS/Python SDKs can connect to sidecar instead of embedding

#### CLI
- [ ] `tap sidecar` — start sidecar server
- [ ] `tap deploy --target sidecar` — package as Docker container
- [ ] `tap transform` — validate and test transform code

#### Transports
- [ ] WebSocket sink output (in addition to SSE)
- [ ] gRPC streaming sink output

### Breaking Changes from 0.1.0
- `ChangeEvent` envelope gains `transformed_at` timestamp field.
- `tap dev` now defaults to sidecar mode if `--embedded` is not passed.

### Migration Notes
- 0.1.0 users: add `--embedded` flag to `tap dev` to maintain previous behavior. No data migration needed.

### Success Criteria
- A Python developer can `pip install tap` and capture Postgres changes in a FastAPI app.
- Sidecar mode allows a Go/Ruby/PHP app to consume changes via HTTP/SSE without a native SDK.
- Parallel snapshotting completes a 10M-row table in < 5 minutes (vs. 30+ minutes sequential).

---

## 0.3.0 — "Scale" (Month 3-4)

**Theme:** Cluster mode, shared state, production hardening.

### Deliverables

#### Core Engine
- [ ] Cluster mode with built-in gossip protocol (no etcd/ZK dependency)
- [ ] Postgres-based shared state store (for cluster coordination)
- [ ] Leader election via Postgres advisory locks
- [ ] Shard-per-database assignment across nodes
- [ ] Auto-failover: another node picks up a failed node's slot
- [ ] Backpressure-aware transport with consumer ACK flow control
- [ ] Dead-letter queue (DLQ) with automatic retry + exponential backoff

#### Schema Registry
- [ ] Embedded schema registry (SQLite-backed, cluster-aware via Postgres)
- [ ] DDL event capture as first-class `ChangeEvent` operations
- [ ] Schema version tracking per table
- [ ] Auto-generated migration SQL for downstream sinks
- [ ] Semantic diffing: structured schema change events (not just strings)

#### Transports
- [ ] Redis Streams sink
- [ ] NATS JetStream sink
- [ ] AWS Kinesis sink
- [ ] Google Pub/Sub sink
- [ ] Kafka sink (finally, but as one of many, not the default)

#### CLI
- [ ] `tap cluster join` — join a cluster
- [ ] `tap cluster status` — view node health and shard assignments
- [ ] `tap cluster leave` — graceful node departure
- [ ] `tap migrate` — generate downstream migration SQL from schema changes

#### Observability
- [ ] OpenTelemetry integration (traces + metrics)
- [ ] Structured logging with configurable levels
- [ ] Dashboard: built-in web UI at `:9911/dashboard` showing capture lag, throughput, schema versions

### Breaking Changes from 0.2.0
- State store format changes from flat SQLite to versioned schema. Auto-migration on first run.
- `tap dev` no longer supports `--embedded` for cluster features; sidecar is required for cluster mode.

### Migration Notes
- 0.2.0 users: state DB auto-migrates on first 0.3.0 startup. Backup recommended.
- Sidecar mode becomes the default for all deployments.

### Success Criteria
- 3-node cluster survives node failure without data loss or duplicate events.
- Schema change in source DB automatically generates `ALTER TABLE` for sink within 30 seconds.
- DLQ handles 100% of sink failures with < 1s retry latency.

---

## 0.4.0 — "AI-Native" (Month 4-5)

**Theme:** MCP server, natural language pipelines, AI agent integration.

### Deliverables

#### MCP Server (`tap-mcp`)
- [ ] Model Context Protocol server exposing all Tap operations
- [ ] AI agent can: discover schemas, generate pipelines, query metrics, debug lag, suggest fixes
- [ ] `tools/list` endpoint: `capture`, `inspect`, `getMetrics`, `controlStream`, `generatePipeline`
- [ ] `resources/list` endpoint: live schema catalog, active pipelines, node status

#### Natural Language Pipeline Generation
- [ ] `tap.ai.generate()` API in TS/Python SDKs
- [ ] Prompt-to-pipeline with human approval gate (`review: true`)
- [ ] Generated pipelines include transforms, sinks, and error handling
- [ ] Feedback loop: accept/reject generated pipelines to improve future generations

#### Go SDK (`sdk-go`)
- [ ] Native embedded mode via CGO to C FFI
- [ ] Channel-based streaming (`<-chan ChangeEvent`)
- [ ] Context cancellation and deadline support
- [ ] Static binary compilation

#### Elixir SDK (`sdk-elixir`)
- [ ] Native NIF via Rustler
- [ ] GenStage producer integration
- [ ] Backpressure via `handle_demand`

#### Edge / WASM
- [ ] Full capture engine compiled to WASM (`tap-wasm`)
- [ ] Cloudflare Workers / Deno Deploy compatibility
- [ ] WebSocket proxy for Postgres logical decoding in edge environments

#### CLI
- [ ] `tap ai` — interactive AI assistant for pipeline design
- [ ] `tap mcp` — start MCP server

### Breaking Changes from 0.3.0
- None. This is a purely additive release.

### Success Criteria
- An AI agent (e.g., Claude, Cursor) can design a complete CDC pipeline from a single sentence and deploy it with human approval.
- Go developer can `go get github.com/tap/sdk-go` and embed Tap in a service.
- Elixir developer can use Tap as a GenStage producer in a Flow pipeline.
- WASM build runs in Cloudflare Worker with < 50ms cold start.

---

## 0.5.0 — "Ecosystem" (Month 5-6)

**Theme:** Community transforms, connector marketplace, advanced SDKs.

### Deliverables

#### Transform Marketplace
- [ ] WASM transform registry (community-published transforms)
- [ ] `tap transform install <name>` — install from registry
- [ ] Verified transforms: PII masking, GDPR anonymization, field encryption, event routing
- [ ] Transform testing framework with fixtures and assertions

#### Java / Kotlin / Scala SDK (`sdk-java`)
- [ ] gRPC sidecar client (primary)
- [ ] Spring Boot Starter (`@EnableTap`, auto-configuration)
- [ ] Micronaut integration (`@TapListener`)
- [ ] Reactor/RxJava `Flux<ChangeEvent>` adapter

#### C# / .NET SDK (`sdk-dotnet`)
- [ ] gRPC sidecar client (primary)
- [ ] `IAsyncEnumerable` streaming
- [ ] `IHostedService` for ASP.NET Core
- [ ] `IObservable` adapter for Rx.NET

#### Ruby / PHP SDKs (`sdk-ruby`, `sdk-php`)
- [ ] gRPC sidecar clients
- [ ] Idiomatic APIs: blocks in Ruby, `foreach` in PHP

#### Connectors
- [ ] MongoDB oplog adapter
- [ ] SQLite WAL adapter (for edge/mobile use cases)

#### CLI
- [ ] `tap transform publish` — publish to community registry
- [ ] `tap connector list` — list available source connectors
- [ ] `tap benchmark` — built-in throughput/latency benchmark tool

### Breaking Changes from 0.4.0
- None. Additive release.

### Success Criteria
- 10+ community transforms available in registry.
- Java Spring Boot app can consume CDC events with 3 lines of config.
- Benchmark tool shows > 10,000 events/sec throughput on single node.

---

## 0.6.0 — "Enterprise Connectors" (Month 6-7)

**Theme:** The databases enterprises actually use.

### Deliverables

#### Core Connectors
- [ ] SQL Server CDC adapter (change tracking / CDC tables)
- [ ] Oracle LogMiner adapter
- [ ] CockroachDB adapter (uses Postgres protocol but with nuances)
- [ ] ClickHouse adapter (for sink, not source)

#### Enterprise Features
- [ ] RBAC: role-based access control for API and dashboard
- [ ] Audit logging: every schema change, pipeline modification, access event logged
- [ ] Encryption at rest for state store and DLQ
- [ ] TLS/mTLS for all transport layers (gRPC, HTTP, sidecar communication)
- [ ] Secret management: integration with HashiCorp Vault, AWS Secrets Manager, Azure Key Vault

#### Debezium Migration Bridge
- [ ] `tap migrate debezium` — read Debezium `SourceRecord` format and emit Tap `ChangeEvent`
- [ ] Drop-in adapter for gradual migration from Debezium to Tap
- [ ] Compatibility mode: preserves Debezium envelope shape for existing consumers

#### CLI
- [ ] `tap auth` — manage RBAC users and roles
- [ ] `tap audit` — query audit logs
- [ ] `tap migrate` — migration tooling

### Breaking Changes from 0.5.0
- Dashboard now requires authentication (RBAC enabled by default).
- Sidecar communication defaults to TLS in production mode.

### Migration Notes
- 0.5.0 users: add `--insecure` to `tap sidecar` for local dev, or provide TLS certs for production.

### Success Criteria
- Fortune 500 can adopt Tap for SQL Server/Oracle without replacing existing Kafka infrastructure.
- Debezium migration bridge processes 1M events with zero data loss.
- SOC 2 Type II readiness checklist complete.

---

## 0.7.0 — "Cloud-Native" (Month 7-8)

**Theme:** Serverless, managed offerings, operational excellence.

### Deliverables

#### Serverless Mode
- [ ] Stateless capture agents: compute is ephemeral, state lives in S3/R2
- [ ] Auto-scaling: scale from 0 to N based on capture lag
- [ ] Per-event billing model (no provisioned cluster costs)
- [ ] Cold start < 200ms for serverless agents

#### Kubernetes Operator
- [ ] `tap-operator` Helm chart
- [ ] Custom Resource Definitions (CRDs): `TapCapture`, `TapSink`, `TapTransform`
- [ ] Auto-scaling via HPA based on capture lag metrics
- [ ] Rolling updates with zero-downtime slot handoff
- [ ] Prometheus ServiceMonitor integration

#### Terraform Provider
- [ ] `tap_capture` resource
- [ ] `tap_sink` resource
- [ ] `tap_transform` resource
- [ ] Data sources for schema inspection

#### Managed Cloud (Beta)
- [ ] Hosted Tap control plane
- [ ] Web UI for pipeline design (no-code + code views)
- [ ] Managed Postgres state store (backed by cloud provider)
- [ ] 99.9% SLA for managed control plane

#### CLI
- [ ] `tap cloud login` — authenticate with managed service
- [ ] `tap cloud deploy` — deploy to managed Tap Cloud
- [ ] `tap k8s` — Kubernetes-specific commands

### Breaking Changes from 0.6.0
- None. Additive release.

### Success Criteria
- Serverless deployment costs 80% less than provisioned for low-traffic workloads.
- Kubernetes operator manages 50+ capture pipelines with zero manual intervention.
- Managed Cloud beta has 100+ active users.

---

## 0.8.0 — "Performance" (Month 8-9)

**Theme:** Make it fast. Make it cheap.

### Deliverables

#### Core Engine Optimization
- [ ] Zero-copy parsing for all connectors (eliminate remaining allocations)
- [ ] SIMD-optimized JSON serialization for event envelopes
- [ ] Memory-mapped state store for hot paths
- [ ] Lock-free LSN tracking via atomics

#### Batch Processing
- [ ] Configurable batch sizes and flush intervals
- [ ] Micro-batching for high-throughput sinks (Kafka, Kinesis)
- [ ] Batch compression: zstd, lz4, snappy

#### Advanced Transforms
- [ ] Windowed aggregations in WASM (tumbling, sliding, session windows)
- [ ] Join transforms: stream-to-stream joins within time windows
- [ ] Lookup transforms: enrich events from external HTTP/Redis sources

#### Cost Optimization
- [ ] Intelligent snapshotting: pause during low-traffic hours, resume during peaks
- [ ] Selective column capture: only capture changed columns, not full row images
- [ ] Event deduplication at source before transport

#### CLI
- [ ] `tap profile` — CPU/memory profiling for pipelines
- [ ] `tap optimize` — suggest config changes based on workload patterns

### Breaking Changes from 0.7.0
- Default batch size changes from 1 (immediate flush) to 100 (1ms flush interval). Users requiring immediate flush must set `batch.size: 1`.

### Migration Notes
- 0.7.0 users: add `batch.size: 1` to preserve previous latency behavior, or accept the new default for better throughput.

### Success Criteria
- Single-node throughput: > 50,000 events/sec for Postgres CDC.
- Memory footprint: < 20MB for idle sidecar, < 100MB under full load.
- Cost per million events: <$0.01 in serverless mode.

---

## 0.9.0 — "Hardening" (Month 9-10)

**Theme:** Production-grade stability, security, compliance.

### Deliverables

#### Security
- [ ] SOC 2 Type II certification complete
- [ ] GDPR compliance: data retention policies, right-to-erasure for captured events
- [ ] HIPAA BAA available for managed cloud
- [ ] FIPS 140-2 compliant crypto modules (optional build flag)
- [ ] Penetration testing and public bug bounty program

#### Reliability
- [ ] Chaos engineering suite: random node kills, network partitions, disk failures
- [ ] Automatic corruption detection and repair for state store
- [ ] Long-haul testing: 30-day continuous capture with 99.999% durability
- [ ] Graceful degradation: continue capturing even if sink is down (unbounded DLQ with disk spill)

#### Documentation
- [ ] Complete API reference for all SDKs
- [ ] Architecture decision records (ADRs) for all major design choices
- [ ] Runbooks for common operational scenarios
- [ ] Video tutorial series (10+ videos)

#### Community
- [ ] Public Discord/Slack community with 1,000+ members
- [ ] Monthly community calls
- [ ] Contributor ladder and maintainer governance model
- [ ] First external committer with merge rights

### Breaking Changes from 0.8.0
- None. This is a stability release.

### Success Criteria
- 30-day chaos test passes with zero data loss.
- 10+ external contributors with merged PRs.
- SOC 2 Type II report available to customers.

---

## 1.0.0 — "Stable" (Month 10-12)

**Theme:** The promise. Backward compatibility. Enterprise trust.

### Deliverables

#### API Stability
- [ ] Core API frozen: `ChangeEvent` envelope, SDK APIs, CLI commands
- [ ] Backward compatibility guarantee: 1.x releases will not break 1.0.0 APIs
- [ ] Deprecation policy: 2-release warning before any API removal
- [ ] LTS branch: 1.0.x receives security patches for 2 years

#### Ecosystem Completeness
- [ ] All Tier 1 SDKs (TS, Python, Go, Rust, Elixir) at feature parity
- [ ] All Tier 2 SDKs (Java, C#) at production quality
- [ ] All Tier 3 SDKs (Ruby, PHP) at community quality
- [ ] 20+ source connectors (Postgres, MySQL, SQL Server, Oracle, MongoDB, SQLite, CockroachDB, and more)
- [ ] 10+ sink transports (HTTP, WebSocket, gRPC, Redis, NATS, Kafka, Kinesis, Pub/Sub, ClickHouse, and more)

#### Managed Cloud (GA)
- [ ] 99.99% SLA
- [ ] Multi-region deployment
- [ ] SSO/SAML integration
- [ ] Usage-based billing with cost caps and alerts
- [ ] Professional support plans (Standard, Business, Enterprise)

#### Governance
- [ ] Transition to neutral foundation (CNCF or Apache Software Foundation)
- [ ] Technical Steering Committee (TSC) with 5+ members
- [ ] Code of Conduct, Contribution Guidelines, Security Policy
- [ ] Trademark and brand guidelines

### Breaking Changes
- **None.** This is the stability promise.

### Success Criteria
- 100+ production deployments.
- 5,000+ GitHub stars.
- $1M ARR for managed cloud.
- Foundation acceptance letter.

---

## Post-1.0 Roadmap (Vision)

### 1.1.0 — "Multi-Region"
- Cross-region replication with conflict-free replicated data types (CRDTs)
- Geo-partitioned capture for global databases
- Latency-aware routing to nearest capture node

### 1.2.0 — "Analytics"
- Built-in materialized views from CDC streams
- SQL-over-CDC: query live change streams with SQL
- Integration with Apache Flink, Materialize, RisingWave

### 1.3.0 — "Edge Mesh"
- P2P capture mesh for edge devices
- SQLite sync across thousands of mobile/IoT devices
- Conflict resolution for offline-first applications

### 2.0.0 — "The Platform"
- Event sourcing as a service: full CQRS/ES platform built on Tap
- Temporal queries: "What did the database look like at 2026-05-31 13:00:00?"
- Time-travel debugging for AI agents

---

## Version Dependency Matrix

| Version | Rust Core | moon | Node | Python | Go | Notes |
|---------|-----------|------|------|--------|-----|-------|
| 0.1.0 | 1.85.0 | 1.32.0 | 22.5.0 | 3.12.0 | — | Foundation |
| 0.2.0 | 1.85.0 | 1.32.0 | 22.5.0 | 3.12.0 | — | Polyglot |
| 0.3.0 | 1.86.0 | 1.33.0 | 22.6.0 | 3.12.0 | 1.24.0 | Scale |
| 0.4.0 | 1.86.0 | 1.33.0 | 22.6.0 | 3.12.0 | 1.24.0 | AI-Native |
| 0.5.0 | 1.87.0 | 1.34.0 | 22.7.0 | 3.12.0 | 1.24.0 | Ecosystem |
| 0.6.0 | 1.87.0 | 1.34.0 | 22.7.0 | 3.12.0 | 1.24.0 | Enterprise |
| 0.7.0 | 1.88.0 | 1.35.0 | 22.8.0 | 3.12.0 | 1.25.0 | Cloud-Native |
| 0.8.0 | 1.88.0 | 1.35.0 | 22.8.0 | 3.12.0 | 1.25.0 | Performance |
| 0.9.0 | 1.89.0 | 1.36.0 | 22.9.0 | 3.12.0 | 1.25.0 | Hardening |
| 1.0.0 | 1.90.0 | 1.36.0 | 22.9.0 | 3.12.0 | 1.25.0 | Stable |

---

## Release Cadence

- **Minor releases (0.x.0):** Every 4-6 weeks during 0.x phase
- **Patch releases (0.x.y):** As needed for critical bugs and security fixes
- **Release candidates:** 1-week RC period for 0.x.0 releases before GA
- **Changelog:** Keep a `CHANGELOG.md` following [Keep a Changelog](https://keepachangelog.com/) format
- **Migration guides:** Every minor release with breaking changes gets a dedicated migration doc

---

*Generated from brainstorming session on 2026-05-31. Subject to change based on community feedback and development velocity.*
