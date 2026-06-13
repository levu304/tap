//! MySQL binlog source connector.
//!
//! Implements the source side of the CDC pipeline for MySQL, following the
//! same architectural patterns as the [`crate::postgres`] module.
//!
//! # Module structure
//!
//! | File | Responsibility |
//! |------|----------------|
//! | [`connection`] | TCP/TLS connection to MySQL, pre-flight checks |
//! | [`events`]     | `MySqlChangeEvent`, binlog event parsing stubs |
//! | [`types`]      | `mysql_async::Value` → `serde_json::Value` mapping |
//! | [`snapshot`]   | Parallel snapshot engine with FTWRL protocol |
//!
//! # Status
//!
//! This module provides connection management, schema resolution, event
//! structure definitions, and a parallel snapshot engine.  *Binlog streaming
//! itself is deferred to a later phase* — the `parse_binlog_event` function
//! in [`events`] is a placeholder that illustrates how row events will be
//! converted into `MySqlChangeEvent` values once a stream is established.

pub mod connection;
pub mod events;
pub mod schema;
pub mod snapshot;
pub mod types;

pub use events::MySqlChangeEvent;
pub use types::ColumnInfo;
