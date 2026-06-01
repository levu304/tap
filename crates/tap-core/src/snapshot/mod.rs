//! Snapshot Engine — sequential table snapshots with `pg_export_snapshot()`.
//!
//! This module implements the P6 Snapshot Runner, which performs a
//! consistent snapshot of the configured Postgres tables using
//! `pg_export_snapshot()` and emits `op:'r'` (Read) [`ChangeEvent`]s
//! per row.
//!
//! # Architecture
//!
//! 1. Export a global snapshot via `pg_export_snapshot()`, recording the
//!    current WAL position.
//! 2. For each table (sorted by schema + name), open a REPEATABLE READ
//!    transaction pinned to the exported snapshot.
//! 3. Detect primary-key columns, build an ordered `SELECT`, scan every
//!    row, and emit a [`ChangeEvent`] with `op: Operation::Read` for each.
//! 4. Checkpoint progress to the [`StateStore`] every `batch_size` rows.
//! 5. Return a [`SnapshotResult`] summarising the run.

pub mod runner;

pub use runner::{SnapshotResult, SnapshotRunner};
