//! SQLite-backed state store for checkpoints, offsets, and metadata.
//!
//! Provides a [`StateStore`] backed by WAL-mode rusqlite for persisting
//! replication progress, snapshot status, schema cache, skipped LSNs,
//! and instance metadata across restarts.
//!
//! # Architecture
//!
//! The store holds a single [`rusqlite::Connection`] and is **not** `Send`
//! or `Sync`.  For shared access across async tasks, wrap it in
//! [`std::sync::Mutex`] behind an [`std::sync::Arc`], or keep it on one
//! task and use message passing.
//!
//! # Tables
//!
//! | Table            | Purpose                              |
//! |------------------|--------------------------------------|
//! | `schema_version` | Migration version tracking           |
//! | `offsets`        | Committed LSN checkpoints            |
//! | `snapshots`      | Table snapshot progress              |
//! | `schema_cache`   | Cached table schemas                 |
//! | `skipped_lsns`   | LSNs that failed to process          |
//! | `instance_info`  | Key–value instance metadata          |

mod migration;
mod store;

pub use migration::migrate;
pub use store::{OffsetRecord, SchemaRecord, SnapshotRecord, StateStore};
