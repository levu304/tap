//! Snapshot Engine — sequential and parallel table snapshots.
//!
//! # Modules
//!
//! * [`runner`] — Sequential snapshot via `pg_export_snapshot()` (legacy).
//! * [`parallel`] — Parallel, chunked snapshot with PK-range splitting.
//! * [`chunker`] — PK-range chunk types and generation logic.
//!
//! # Architecture (parallel)
//!
//! 1. Export a global snapshot via `pg_export_snapshot()`, recording the
//!    current WAL position.
//! 2. For each table (sorted by schema + name), divide the PK range into
//!    `num_workers × 2` chunks.
//! 3. N worker tasks compete for chunks from a shared work queue, each
//!    opening a REPEATABLE READ transaction pinned to the exported snapshot.
//! 4. Each worker scans its chunk via server-side cursor and emits
//!    [`ChangeEvent`]s with `op: Operation::Read`.
//! 5. Progress is checkpointed per-chunk to the [`StateStore`].
//! 6. Return a [`SnapshotResult`] summarising the run.

pub mod chunker;
pub mod parallel;
pub mod runner;

pub use chunker::{ChunkStatus, SnapshotChunk};
pub use parallel::run_parallel_snapshot;
pub use runner::{SnapshotResult, SnapshotRunner};
